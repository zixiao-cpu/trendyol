package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trendyol/internal/bypass"
	"trendyol/internal/client"
	"trendyol/internal/model"
	trend "trendyol/internal/trendyol"
)

type fakeSession struct {
	result    *client.Result
	err       error
	rotations []string
}

func (f *fakeSession) Get(context.Context, string) (*client.Result, error) { return f.result, f.err }
func (f *fakeSession) Rotate(reason string) error {
	f.rotations = append(f.rotations, reason)
	return nil
}
func (f *fakeSession) Close()           {}
func (f *fakeSession) ProxyRaw() string { return "http://masked-proxy" }

type fakeBypass struct {
	result *bypass.Result
	err    error
}

func (f *fakeBypass) Fetch(context.Context, string, string) (*bypass.Result, error) {
	return f.result, f.err
}

func workerFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func workerSettings() Settings {
	return Settings{PricePolicy: trend.PricePolicy{MinUSD: 20, TRYPerUSD: 48.066}, CloudBypassEnabled: true, CloudBypassMaxAttempts: 2}
}

func withDescription(body []byte) []byte {
	jsonLD := `<script type="application/ld+json">{"@type":"Product","description":"Complete product detail text"}</script>`
	value := string(body)
	if strings.Contains(value, "</head>") {
		return []byte(strings.Replace(value, "</head>", jsonLD+"</head>", 1))
	}
	return []byte(strings.Replace(value, "<body>", "<body>"+jsonLD, 1))
}

func TestDirectRoutesOnlyExplicitChallengeToBypass(t *testing.T) {
	job := model.Job{ID: 1, Path: "/p-p-1", URL: "https://www.trendyol.com/p-p-1", QueueName: "direct"}
	session := &fakeSession{result: &client.Result{Status: 200, Body: workerFixture(t, "challenge_200.html")}}
	result := ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultChallenge {
		t.Fatalf("kind=%s", result.Kind)
	}
	session.result = &client.Result{Status: 403, Body: []byte("ordinary 403")}
	result = ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultRetry {
		t.Fatalf("ordinary 403 kind=%s", result.Kind)
	}
}

func TestParseFailureRetriesOnceAndSavesSanitizedFixture(t *testing.T) {
	job := model.Job{ID: 1, Path: "/p-p-1", URL: "https://www.trendyol.com/p-p-1", QueueName: "direct"}
	body := []byte(`<html>{"email":"person@example.com","address":"secret"}</html>`)
	session := &fakeSession{result: &client.Result{Status: 200, Body: body}}
	result := ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultParserFailure || len(result.Fixture) == 0 {
		t.Fatalf("result=%+v", result)
	}
	if string(result.Fixture) == string(body) {
		t.Fatal("fixture was not sanitized")
	}
	job.QualityAttempts = 1
	result = ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultSkip || result.Reason != "parser_incompatible" {
		t.Fatalf("second result=%+v", result)
	}
}

func TestQualityRetryThenSafeDegrade(t *testing.T) {
	job := model.Job{ID: 1, Path: "/missing-p-900000003", URL: "https://www.trendyol.com/missing-p-900000003", QueueName: "direct"}
	session := &fakeSession{result: &client.Result{Status: 200, Body: withDescription(workerFixture(t, "missing_variant_value.html"))}}
	result := ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultQualityRetry {
		t.Fatalf("first kind=%s reason=%s", result.Kind, result.Reason)
	}
	job.QualityAttempts = 1
	result = ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultDone || result.Bundle == nil || len(result.Bundle.Variants) != 2 {
		t.Fatalf("second result=%+v", result)
	}
	for _, variant := range result.Bundle.Variants {
		if variant.VariationTheme != "" || variant.VariationValue != "" || variant.VariationStatus != "variation_incomplete" {
			t.Fatalf("fake variation: %+v", variant)
		}
	}
}

func TestCloudBypassQueueHasOwnAttemptLimitAndMetricsPayload(t *testing.T) {
	job := model.Job{ID: 1, Path: "/single-p-881465224", URL: "https://www.trendyol.com/single-p-881465224", QueueName: "cloudbypass", BypassAttempts: 1}
	session := &fakeSession{}
	fetcher := &fakeBypass{result: &bypass.Result{Status: 200, Body: withDescription(workerFixture(t, "single_sku.html")), Duration: 25 * time.Millisecond, Cost: 3}}
	result := ScrapeBypass(context.Background(), fetcher, session, job, workerSettings())
	if result.Kind != model.ResultDone || result.BypassCost != 3 || result.Duration != 25*time.Millisecond {
		t.Fatalf("result=%+v", result)
	}
	job.BypassAttempts = 3
	result = ScrapeBypass(context.Background(), fetcher, session, job, workerSettings())
	if result.Kind != model.ResultSkip {
		t.Fatalf("exhausted result=%+v", result)
	}
}

func TestMissingDescriptionRetriesThenSkips(t *testing.T) {
	job := model.Job{ID: 1, Path: "/single-p-881465224", URL: "https://www.trendyol.com/single-p-881465224", QueueName: "direct"}
	session := &fakeSession{result: &client.Result{Status: 200, Body: workerFixture(t, "single_sku.html")}}
	result := ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultQualityRetry || result.Reason != "description_missing" {
		t.Fatalf("first result=%+v", result)
	}
	job.QualityAttempts = 1
	result = ScrapeDirect(context.Background(), session, job, workerSettings())
	if result.Kind != model.ResultSkip || result.Reason != "description_missing" {
		t.Fatalf("second result=%+v", result)
	}
}

func TestCloudBypassBudgetRejectionIsNotCountedAsAPaidAttempt(t *testing.T) {
	job := model.Job{ID: 1, Path: "/single-p-881465224", QueueName: "cloudbypass", BypassAttempts: 1}
	result := ScrapeBypass(context.Background(), &fakeBypass{err: bypass.ErrDisabled}, &fakeSession{}, job, workerSettings())
	if result.Kind != model.ResultSkip || result.Reason != "cloudbypass_budget_exhausted" || result.BypassAttempted {
		t.Fatalf("result=%+v", result)
	}
}

func TestCloudBypassNetworkFailurePersistsReservedCost(t *testing.T) {
	job := model.Job{ID: 1, Path: "/single-p-881465224", QueueName: "cloudbypass", BypassAttempts: 1}
	fetcher := &fakeBypass{result: &bypass.Result{Duration: 40 * time.Millisecond, Cost: 3}, err: context.DeadlineExceeded}
	result := ScrapeBypass(context.Background(), fetcher, &fakeSession{}, job, workerSettings())
	if result.Kind != model.ResultRetry || !result.BypassAttempted || result.BypassCost != 3 || result.Duration != 40*time.Millisecond {
		t.Fatalf("result=%+v", result)
	}
}
