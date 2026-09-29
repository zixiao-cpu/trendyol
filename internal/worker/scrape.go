package worker

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"trendyol/internal/bypass"
	"trendyol/internal/client"
	"trendyol/internal/model"
	trend "trendyol/internal/trendyol"
)

type Settings struct {
	PricePolicy            trend.PricePolicy
	Excluded               []string
	CloudBypassEnabled     bool
	CloudBypassMaxAttempts int
}

type DirectSession interface {
	Get(context.Context, string) (*client.Result, error)
	Rotate(string) error
	Close()
	ProxyRaw() string
}

type BypassFetcher interface {
	Fetch(context.Context, string, string) (*bypass.Result, error)
}

func ScrapeDirect(ctx context.Context, session DirectSession, job model.Job, settings Settings) model.ScrapeResult {
	started := time.Now()
	result := model.ScrapeResult{Job: job, Duration: time.Since(started)}
	response, err := session.Get(ctx, job.URL)
	if err != nil {
		result.Kind, result.Reason = model.ResultRetry, "network_error"
		return result
	}
	result.HTTPStatus, result.ResponseBytes, result.Duration, result.ProxyKey = response.Status, response.Bytes, response.Duration, response.ProxyKey
	switch trend.ClassifyResponse(response.Status, response.Body) {
	case trend.ResponseChallenge:
		_ = session.Rotate("challenge")
		if settings.CloudBypassEnabled {
			result.Kind, result.Reason = model.ResultChallenge, "explicit_challenge"
		} else {
			result.Kind, result.Reason = model.ResultRetry, "challenge_bypass_disabled"
		}
		return result
	case trend.ResponseGeoGate:
		result.Kind, result.Reason = model.ResultRetry, "country_selection_gate"
		_ = session.Rotate(result.Reason)
		return result
	case trend.ResponseRetry:
		result.Kind, result.Reason = model.ResultRetry, fmt.Sprintf("http_%d", response.Status)
		_ = session.Rotate(result.Reason)
		return result
	case trend.ResponseNotFound:
		result.Kind, result.Reason = model.ResultSkip, "not_found"
		return result
	case trend.ResponseTerminal:
		result.Kind, result.Reason = model.ResultSkip, fmt.Sprintf("terminal_http_%d", response.Status)
		return result
	}
	return parseResponse(session, job, response.Body, result, settings)
}

func ScrapeBypass(ctx context.Context, fetcher BypassFetcher, proxySession DirectSession, job model.Job, settings Settings) model.ScrapeResult {
	result := model.ScrapeResult{Job: job}
	if !settings.CloudBypassEnabled || job.BypassAttempts > settings.CloudBypassMaxAttempts {
		result.Kind, result.Reason = model.ResultSkip, "cloudbypass_disabled_or_exhausted"
		return result
	}
	response, err := fetcher.Fetch(ctx, job.Path, proxySession.ProxyRaw())
	if err != nil {
		if errors.Is(err, bypass.ErrDisabled) {
			result.Kind, result.Reason = model.ResultSkip, "cloudbypass_budget_exhausted"
			return result
		}
		result.BypassAttempted = true
		if response != nil {
			result.HTTPStatus = response.Status
			result.ResponseBytes = int64(len(response.Body))
			result.Duration = response.Duration
			result.BypassCost = response.Cost
		}
		result.Kind, result.Reason = model.ResultRetry, "cloudbypass_error"
		return result
	}
	result.BypassAttempted = true
	result.HTTPStatus, result.ResponseBytes, result.Duration, result.BypassCost = response.Status, int64(len(response.Body)), response.Duration, response.Cost
	if response.Status != 200 {
		result.Kind, result.Reason = model.ResultRetry, fmt.Sprintf("cloudbypass_http_%d", response.Status)
		return result
	}
	classification := trend.ClassifyResponse(200, response.Body)
	if classification == trend.ResponseChallenge {
		result.Kind, result.Reason = model.ResultRetry, "cloudbypass_challenge_persisted"
		return result
	}
	if classification == trend.ResponseGeoGate {
		result.Kind, result.Reason = model.ResultRetry, "cloudbypass_country_selection_gate"
		return result
	}
	return parseResponse(proxySession, job, response.Body, result, settings)
}

func parseResponse(session DirectSession, job model.Job, body []byte, result model.ScrapeResult, settings Settings) model.ScrapeResult {
	bundle, info, err := trend.ParseHTML(body, job.Path, "", settings.PricePolicy, settings.Excluded, job.QualityAttempts >= 1)
	if err != nil {
		var parseError *trend.ParseError
		reason := "parser_incompatible"
		if errors.As(err, &parseError) {
			reason = parseError.Reason
		}
		switch reason {
		case "parser_incompatible":
			if job.QualityAttempts < 1 {
				_ = session.Rotate(reason)
				result.Kind, result.Reason, result.Fixture = model.ResultParserFailure, reason, SanitizeFixture(body)
			} else {
				result.Kind, result.Reason = model.ResultSkip, reason
			}
		case "variation_incomplete", "sku_price_missing":
			if job.QualityAttempts < 1 {
				_ = session.Rotate(reason)
				result.Kind, result.Reason = model.ResultQualityRetry, reason
			} else {
				result.Kind, result.Reason = model.ResultSkip, reason
			}
		default:
			result.Kind, result.Reason = model.ResultSkip, reason
		}
		return result
	}
	if (info.VariationIncomplete || len(info.MissingPriceSKUs) > 0) && job.QualityAttempts < 1 {
		_ = session.Rotate("quality_retry")
		result.Kind, result.Reason = model.ResultQualityRetry, "incomplete_variant_data"
		return result
	}
	if bundle == nil || strings.TrimSpace(bundle.Product.Description) == "" {
		if job.QualityAttempts < 1 {
			_ = session.Rotate("description_missing")
			result.Kind, result.Reason = model.ResultQualityRetry, "description_missing"
		} else {
			result.Kind, result.Reason = model.ResultSkip, "description_missing"
		}
		return result
	}
	result.Kind, result.Bundle, result.Reason = model.ResultDone, bundle, ""
	return result
}

var (
	secretField = regexp.MustCompile(`(?i)"(?:taxNumber|registrationNumber|address|email|phone)"\s*:\s*"[^"]*"`)
	emailValue  = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
)

func SanitizeFixture(body []byte) []byte {
	value := secretField.ReplaceAllStringFunc(string(body), func(field string) string {
		index := strings.Index(field, ":")
		return field[:index+1] + `"[REDACTED]"`
	})
	value = emailValue.ReplaceAllString(value, "[REDACTED_EMAIL]")
	if len(value) > 2<<20 {
		value = value[:2<<20]
	}
	return []byte(value)
}
