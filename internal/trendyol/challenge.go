package trendyol

import (
	"regexp"
	"strings"
)

type ResponseClass string

const (
	ResponseNormal    ResponseClass = "normal"
	ResponseChallenge ResponseClass = "challenge"
	ResponseGeoGate   ResponseClass = "geo_gate"
	ResponseRetry     ResponseClass = "retry"
	ResponseNotFound  ResponseClass = "not_found"
	ResponseTerminal  ResponseClass = "terminal"
)

var captchaElement = regexp.MustCompile(`(?i)<title[^>]*>[^<]*captcha|(?:id|class)=["'][^"']*(?:captcha|challenge)`)

func ClassifyResponse(status int, body []byte) ResponseClass {
	sample := strings.ToLower(string(body))
	if len(sample) > 200000 {
		sample = sample[:200000]
	}
	for _, marker := range []string{"attention required", "cf-chl", "verify you are human", "cloudflare ray id", "challenge-platform", "turnstile"} {
		if strings.Contains(sample, marker) {
			return ResponseChallenge
		}
	}
	if captchaElement.MatchString(sample) {
		return ResponseChallenge
	}
	for _, marker := range []string{`"pagetype":"select_country"`, `"page_type":"select_country"`, "__m-country-selection__props", "/en/select-country"} {
		if strings.Contains(sample, marker) {
			return ResponseGeoGate
		}
	}
	if status == 404 || status == 410 {
		return ResponseNotFound
	}
	if status == 403 || status == 408 || status == 425 || status == 429 || status == 503 || status >= 500 {
		return ResponseRetry
	}
	if status >= 400 {
		return ResponseTerminal
	}
	return ResponseNormal
}
