package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DBPath                    string
	OutputDir                 string
	OutputPath                string
	ProductTarget             int
	CategoryProductCap        int
	MinSellingPriceUSD        float64
	TRYPerUSD                 float64
	FXRateAsOf                string
	ExcludedCategoryTerms     []string
	Concurrency               int
	BypassConcurrency         int
	RequestTimeout            time.Duration
	MaxAttempts               int
	RetryBase                 time.Duration
	ProxyURLs                 []string
	ProxyTemplate             string
	ProxyTrafficBudgetGB      float64
	ProxyStopReserveGB        float64
	StickyPages               int
	StickyPagesJitter         int
	CloudBypassEnabled        bool
	CloudBypassAPIKey         string
	CloudBypassMaxAttempts    int
	CloudBypassMaxConcurrency int
	CloudBypassBudget         float64
	PendingLow                int
	PendingTarget             int
	FreshPendingLow           int
	FreshPendingTarget        int
	SitemapMax                int
	ClaimBatch                int
	LeaseDuration             time.Duration
	FixtureDir                string
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	if err := validateEnvironmentSyntax(); err != nil {
		return nil, err
	}
	c := &Config{
		DBPath:                    getenv("DB_PATH", "state/trendyol.db"),
		OutputDir:                 getenv("OUTPUT_DIR", "out"),
		ProductTarget:             preferredInt("PRODUCT_TARGET", "TOTAL_TARGET", 1000000),
		CategoryProductCap:        preferredInt("CATEGORY_PRODUCT_CAP", "CATEGORY_CAP", 0),
		MinSellingPriceUSD:        getenvFloat("MIN_SELLING_PRICE_USD", 20),
		TRYPerUSD:                 getenvFloat("TRY_PER_USD", 48.066),
		FXRateAsOf:                getenv("FX_RATE_AS_OF", "2026-08-21"),
		ExcludedCategoryTerms:     splitList(getenv("EXCLUDED_CATEGORY_TERMS", "mücevher|takı|jewelry|jewellery")),
		Concurrency:               getenvInt("CONCURRENCY", 256),
		BypassConcurrency:         getenvInt("BYPASS_CONCURRENCY", 2),
		RequestTimeout:            time.Duration(getenvInt("REQUEST_TIMEOUT", 45)) * time.Second,
		MaxAttempts:               getenvInt("MAX_ATTEMPTS", 6),
		RetryBase:                 time.Duration(getenvInt("RETRY_BASE_SECONDS", 15)) * time.Second,
		ProxyURLs:                 splitList(os.Getenv("PROXY_URLS")),
		ProxyTemplate:             strings.TrimSpace(os.Getenv("PROXY_TEMPLATE")),
		ProxyTrafficBudgetGB:      getenvFloat("PROXY_TRAFFIC_BUDGET_GB", 0),
		ProxyStopReserveGB:        getenvFloat("PROXY_STOP_RESERVE_GB", 0),
		StickyPages:               getenvInt("STICKY_PAGES", 24),
		StickyPagesJitter:         getenvInt("STICKY_PAGES_JITTER", 8),
		CloudBypassEnabled:        getenvBool("CLOUDBYPASS_ENABLED", false),
		CloudBypassAPIKey:         strings.TrimSpace(getenv("CLOUDBYPASS_APIKEY", os.Getenv("CB_APIKEY"))),
		CloudBypassMaxAttempts:    getenvInt("CLOUDBYPASS_MAX_ATTEMPTS", 2),
		CloudBypassMaxConcurrency: getenvInt("CLOUDBYPASS_MAX_CONCURRENCY", 2),
		CloudBypassBudget:         getenvFloat("CLOUDBYPASS_BUDGET", 0),
		PendingLow:                getenvInt("PENDING_LOW_WATERMARK", 40000),
		PendingTarget:             getenvInt("PENDING_QUEUE_TARGET", 100000),
		FreshPendingLow:           getenvInt("FRESH_PENDING_LOW_WATERMARK", 10000),
		FreshPendingTarget:        getenvInt("FRESH_PENDING_QUEUE_TARGET", 40000),
		SitemapMax:                getenvInt("SITEMAP_MAX", 301),
		ClaimBatch:                getenvInt("CLAIM_BATCH", 1024),
		LeaseDuration:             time.Duration(getenvInt("LEASE_SECONDS", 180)) * time.Second,
		FixtureDir:                getenv("FAILURE_FIXTURE_DIR", "state/failures"),
	}
	c.OutputPath = filepath.Join(c.OutputDir, "TRENDYOL_SKU_DATA.csv")
	if v := strings.TrimSpace(os.Getenv("OUTPUT_PATH")); v != "" {
		c.OutputPath = v
	}
	if c.Concurrency < 1 || c.MaxAttempts < 1 || c.ClaimBatch < 1 {
		return nil, fmt.Errorf("CONCURRENCY, MAX_ATTEMPTS and CLAIM_BATCH must be positive")
	}
	if c.ProductTarget < 0 || c.CategoryProductCap < 0 || c.MinSellingPriceUSD < 0 {
		return nil, fmt.Errorf("PRODUCT_TARGET, CATEGORY_PRODUCT_CAP and MIN_SELLING_PRICE_USD cannot be negative")
	}
	if c.TRYPerUSD <= 0 {
		return nil, fmt.Errorf("TRY_PER_USD must be positive")
	}
	if c.ProxyTrafficBudgetGB < 0 || c.ProxyStopReserveGB < 0 {
		return nil, fmt.Errorf("PROXY_TRAFFIC_BUDGET_GB and PROXY_STOP_RESERVE_GB cannot be negative")
	}
	if c.ProxyTrafficBudgetGB > 0 && c.ProxyStopReserveGB >= c.ProxyTrafficBudgetGB {
		return nil, fmt.Errorf("PROXY_STOP_RESERVE_GB must be smaller than PROXY_TRAFFIC_BUDGET_GB")
	}
	if c.RequestTimeout <= 0 || c.RetryBase <= 0 || c.LeaseDuration <= 0 {
		return nil, fmt.Errorf("REQUEST_TIMEOUT, RETRY_BASE_SECONDS and LEASE_SECONDS must be positive")
	}
	if c.StickyPages < 1 || c.StickyPagesJitter < 0 {
		return nil, fmt.Errorf("STICKY_PAGES must be positive and STICKY_PAGES_JITTER cannot be negative")
	}
	if c.BypassConcurrency < 0 || c.CloudBypassMaxConcurrency < 0 || c.CloudBypassMaxAttempts < 0 || c.CloudBypassBudget < 0 {
		return nil, fmt.Errorf("bypass concurrency cannot be negative")
	}
	if !c.CloudBypassEnabled {
		c.BypassConcurrency = 0
	} else if c.BypassConcurrency > c.CloudBypassMaxConcurrency {
		c.BypassConcurrency = c.CloudBypassMaxConcurrency
	}
	if c.CloudBypassEnabled && c.BypassConcurrency > 0 && c.CloudBypassMaxAttempts < 1 {
		return nil, fmt.Errorf("CLOUDBYPASS_MAX_ATTEMPTS must be positive when bypass workers are enabled")
	}
	if c.PendingLow < 0 || c.PendingTarget < 0 || c.PendingTarget < c.PendingLow {
		return nil, fmt.Errorf("PENDING_QUEUE_TARGET must be >= PENDING_LOW_WATERMARK")
	}
	if c.FreshPendingLow < 0 || c.FreshPendingTarget < 0 || c.FreshPendingTarget < c.FreshPendingLow {
		return nil, fmt.Errorf("FRESH_PENDING_QUEUE_TARGET must be >= FRESH_PENDING_LOW_WATERMARK")
	}
	if c.SitemapMax < 0 {
		return nil, fmt.Errorf("SITEMAP_MAX cannot be negative")
	}
	for _, path := range []string{filepath.Dir(c.DBPath), c.OutputDir, c.FixtureDir} {
		if path != "" && path != "." {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

func validateEnvironmentSyntax() error {
	for _, key := range []string{
		"PRODUCT_TARGET", "CATEGORY_PRODUCT_CAP", "TOTAL_TARGET", "CATEGORY_CAP", "CONCURRENCY", "BYPASS_CONCURRENCY",
		"REQUEST_TIMEOUT", "MAX_ATTEMPTS", "RETRY_BASE_SECONDS", "STICKY_PAGES",
		"STICKY_PAGES_JITTER", "CLOUDBYPASS_MAX_ATTEMPTS", "CLOUDBYPASS_MAX_CONCURRENCY",
		"PENDING_LOW_WATERMARK", "PENDING_QUEUE_TARGET", "SITEMAP_MAX", "CLAIM_BATCH", "LEASE_SECONDS",
		"FRESH_PENDING_LOW_WATERMARK", "FRESH_PENDING_QUEUE_TARGET",
	} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := strconv.Atoi(value); err != nil {
				return fmt.Errorf("%s must be an integer", key)
			}
		}
	}
	for _, key := range []string{"MIN_SELLING_PRICE_USD", "TRY_PER_USD", "CLOUDBYPASS_BUDGET", "PROXY_TRAFFIC_BUDGET_GB", "PROXY_STOP_RESERVE_GB"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				return fmt.Errorf("%s must be numeric", key)
			}
		}
	}
	if value := strings.ToLower(strings.TrimSpace(os.Getenv("CLOUDBYPASS_ENABLED"))); value != "" {
		switch value {
		case "0", "1", "false", "true", "no", "yes":
		default:
			return fmt.Errorf("CLOUDBYPASS_ENABLED must be true or false")
		}
	}
	return nil
}

func (c *Config) ValidateRun() error {
	if len(c.ProxyURLs) == 0 && c.ProxyTemplate == "" {
		return fmt.Errorf("PROXY_URLS or PROXY_TEMPLATE is required for network commands")
	}
	if c.CloudBypassEnabled && c.BypassConcurrency > 0 && c.CloudBypassAPIKey == "" {
		return fmt.Errorf("CLOUDBYPASS_ENABLED=true requires CLOUDBYPASS_APIKEY")
	}
	return nil
}

func (c *Config) SafeSummary() string {
	return fmt.Sprintf("db=%s output=%s product_target=%d category_product_cap=%d min_usd=%.2f try_per_usd=%.4f fx_as_of=%s concurrency=%d bypass=%d proxies=%d template=%v proxy_traffic_budget_gb=%.3f proxy_stop_reserve_gb=%.3f sticky=%d+0..%d cloudbypass=%v budget=%.2f",
		c.DBPath, c.OutputPath, c.ProductTarget, c.CategoryProductCap, c.MinSellingPriceUSD, c.TRYPerUSD, c.FXRateAsOf,
		c.Concurrency, c.BypassConcurrency, len(c.ProxyURLs), c.ProxyTemplate != "", c.ProxyTrafficBudgetGB, c.ProxyStopReserveGB,
		c.StickyPages, c.StickyPagesJitter, c.CloudBypassEnabled, c.CloudBypassBudget)
}

func splitList(raw string) []string {
	raw = strings.NewReplacer("\r", "", "\n", ",", "|", ",").Replace(raw)
	seen := map[string]bool{}
	var out []string
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return fallback
	}
	return value
}

func preferredInt(primary, legacy string, fallback int) int {
	if value := strings.TrimSpace(os.Getenv(primary)); value != "" {
		number, err := strconv.Atoi(value)
		if err == nil {
			return number
		}
	}
	return getenvInt(legacy, fallback)
}

func getenvFloat(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(key)), 64)
	if err != nil {
		return fallback
	}
	return value
}

func getenvBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}
