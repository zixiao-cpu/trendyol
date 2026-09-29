package config

import (
	"strings"
	"testing"
)

func TestUnifiedEnvironmentAndSafeSummary(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("MIN_SELLING_PRICE_USD", "25")
	t.Setenv("TRY_PER_USD", "48.066")
	t.Setenv("EXCLUDED_CATEGORY_TERMS", "foo|bar")
	t.Setenv("CONCURRENCY", "7")
	t.Setenv("BYPASS_CONCURRENCY", "4")
	t.Setenv("CLOUDBYPASS_ENABLED", "true")
	t.Setenv("CLOUDBYPASS_MAX_CONCURRENCY", "2")
	t.Setenv("CLOUDBYPASS_APIKEY", "super-secret-key")
	t.Setenv("PROXY_TEMPLATE", "http://user:super-secret-password@proxy.example:9000")
	t.Setenv("PROXY_TRAFFIC_BUDGET_GB", "100")
	t.Setenv("PROXY_STOP_RESERVE_GB", "2.5")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinSellingPriceUSD != 25 || cfg.TRYPerUSD != 48.066 || cfg.Concurrency != 7 || cfg.BypassConcurrency != 2 || len(cfg.ExcludedCategoryTerms) != 2 || cfg.ProxyTrafficBudgetGB != 100 || cfg.ProxyStopReserveGB != 2.5 {
		t.Fatalf("config=%+v", cfg)
	}
	summary := cfg.SafeSummary()
	if strings.Contains(summary, "super-secret") || strings.Contains(summary, "user:") {
		t.Fatalf("secret leaked in summary: %s", summary)
	}
}

func TestProxyReserveMustBeBelowBudget(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("PROXY_TRAFFIC_BUDGET_GB", "10")
	t.Setenv("PROXY_STOP_RESERVE_GB", "10")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "PROXY_STOP_RESERVE_GB") {
		t.Fatalf("invalid proxy reserve was accepted: %v", err)
	}
}

func TestDisabledCloudBypassAllowsZeroWorkers(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("CLOUDBYPASS_ENABLED", "false")
	t.Setenv("BYPASS_CONCURRENCY", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BypassConcurrency != 0 {
		t.Fatalf("bypass=%d", cfg.BypassConcurrency)
	}
}

func TestCloudBypassMaxConcurrencyZeroReallyDisablesWorkers(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("CLOUDBYPASS_ENABLED", "true")
	t.Setenv("BYPASS_CONCURRENCY", "4")
	t.Setenv("CLOUDBYPASS_MAX_CONCURRENCY", "0")
	t.Setenv("CLOUDBYPASS_APIKEY", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BypassConcurrency != 0 {
		t.Fatalf("bypass=%d", cfg.BypassConcurrency)
	}
	if err := cfg.ValidateRun(); err == nil || !strings.Contains(err.Error(), "PROXY") {
		t.Fatalf("zero bypass workers should not require an API key; got %v", err)
	}
}

func TestInvalidRuntimeRangesAreRejected(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("MIN_SELLING_PRICE_USD", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("negative minimum price accepted")
	}
}

func TestMalformedEnvironmentValueIsRejected(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("CONCURRENCY", "many")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "CONCURRENCY") {
		t.Fatalf("malformed concurrency was accepted: %v", err)
	}
}

func TestMillionProductDefaultsAndLegacyCompatibility(t *testing.T) {
	t.Setenv("DB_PATH", t.TempDir()+"/db.sqlite")
	t.Setenv("OUTPUT_DIR", t.TempDir())
	t.Setenv("PRODUCT_TARGET", "")
	t.Setenv("CATEGORY_PRODUCT_CAP", "")
	t.Setenv("TOTAL_TARGET", "")
	t.Setenv("CATEGORY_CAP", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ProductTarget != 1000000 || cfg.CategoryProductCap != 0 {
		t.Fatalf("product_target=%d category_product_cap=%d", cfg.ProductTarget, cfg.CategoryProductCap)
	}

	t.Setenv("TOTAL_TARGET", "1234")
	t.Setenv("CATEGORY_CAP", "56")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ProductTarget != 1234 || cfg.CategoryProductCap != 56 {
		t.Fatalf("legacy product_target=%d category_product_cap=%d", cfg.ProductTarget, cfg.CategoryProductCap)
	}
}
