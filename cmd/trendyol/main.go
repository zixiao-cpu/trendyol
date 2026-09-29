package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"trendyol/internal/bypass"
	"trendyol/internal/client"
	"trendyol/internal/config"
	exporter "trendyol/internal/export"
	"trendyol/internal/model"
	"trendyol/internal/monitor"
	"trendyol/internal/proxy"
	"trendyol/internal/store"
	trend "trendyol/internal/trendyol"
	"trendyol/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		return
	}
	command, args := args[0], args[1:]
	if command == "help" || command == "-h" || command == "--help" {
		usage()
		return
	}
	fmt.Println("effective_config:", cfg.SafeSummary())
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	switch command {
	case "init":
		fmt.Printf("SQLite WAL initialized: %s\n", cfg.DBPath)
	case "status":
		status, err := db.Snapshot(ctx)
		if err != nil {
			log.Fatal(err)
		}
		monitor.Enrich(&status, int64(cfg.ProductTarget))
		monitor.EnrichProxyBudget(&status, cfg.ProxyTrafficBudgetGB, cfg.ProxyStopReserveGB)
		if len(args) > 0 && args[0] == "--json" {
			raw, _ := json.Marshal(status)
			fmt.Println(string(raw))
		} else {
			printStatus(status, cfg)
		}
	case "monitor":
		interval, once, err := parseMonitorArgs(args)
		if err != nil {
			log.Fatal(err)
		}
		if err := runMonitor(ctx, db, cfg, interval, once); err != nil && err != context.Canceled {
			log.Fatal(err)
		}
	case "import-urls":
		if len(args) != 1 {
			log.Fatal("usage: trendyol import-urls <file>")
		}
		products, pages, err := trend.ImportURLs(ctx, db, args[0])
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("imported products=%d listing_pages=%d (offline)\n", products, pages)
	case "recover":
		n, err := db.RecoverStale(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("recovered stale processing=%d\n", n)
	case "recover-worker":
		if len(args) != 1 {
			log.Fatal("usage: trendyol recover-worker <worker-id>")
		}
		n, err := db.RecoverWorker(ctx, args[0])
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("recovered worker=%s tasks=%d\n", args[0], n)
	case "reprioritize":
		updated, err := trend.ReprioritizeCandidates(ctx, db)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("reprioritized candidates=%d (offline)\n", updated)
	case "prepare-usd":
		requeued, removed, err := db.PrepareUSDReprice(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("USD migration requeued_jobs=%d removed_non_usd_products=%d (offline)\n", requeued, removed)
	case "prepare-content-backfill":
		requeued, err := db.PrepareContentBackfill(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("content backfill requeued_products=%d preserved_existing_rows=true (offline)\n", requeued)
	case "reset-candidates":
		removed, err := db.ResetCandidates(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("reset candidates removed_jobs=%d preserved_products=true (offline)\n", removed)
	case "normalize-data":
		updated, err := db.NormalizeStandaloneParents(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("normalized standalone_parent_rows=%d (offline)\n", updated)
	case "export":
		output := cfg.OutputPath
		if len(args) > 0 {
			output = args[0]
		}
		manifest, err := exporter.CSV(ctx, db, output, cfg.MinSellingPriceUSD)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("exported rows=%d bytes=%d sha256=%s review_queries=%d -> %s\n", manifest.Rows, manifest.Bytes, manifest.SHA256, manifest.ReviewQueries, output)
	case "validate":
		output := cfg.OutputPath
		if len(args) > 0 {
			output = args[0]
		}
		manifest, err := exporter.ValidateFile(output, cfg.MinSellingPriceUSD)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("valid rows=%d bytes=%d sha256=%s\n", manifest.Rows, manifest.Bytes, manifest.SHA256)
	case "discover":
		mustNetwork(cfg)
		pool, factory := networkFactory(cfg)
		session, err := factory.NewSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
		wake := make(chan struct{}, 1)
		discoverer := newDiscoverer(cfg, db, session, wake)
		added, err := discoverer.Refill(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("progressive discovery added=%d\n", added)
		_ = pool
	case "run":
		mustNetwork(cfg)
		if n, err := db.RecoverStale(ctx); err != nil {
			log.Fatal(err)
		} else if n > 0 {
			fmt.Printf("recovered stale=%d\n", n)
		}
		pool, factory := networkFactory(cfg)
		bypassClient, err := bypass.New(cfg.CloudBypassEnabled, cfg.CloudBypassAPIKey, cfg.RequestTimeout, cfg.CloudBypassBudget)
		if err != nil {
			log.Fatal(err)
		}
		defer bypassClient.Close()
		if spent, spentErr := db.BypassSpent(ctx); spentErr == nil {
			bypassClient.SetPersistedSpent(spent)
		}
		discoverySession, err := factory.NewSession()
		if err != nil {
			log.Fatal(err)
		}
		defer discoverySession.Close()
		wake := make(chan struct{}, 1)
		discoverer := newDiscoverer(cfg, db, discoverySession, wake)
		_ = pool
		if err := worker.Run(ctx, db, cfg, factory, bypassClient, discoverer, wake); err != nil && err != context.Canceled {
			log.Fatal(err)
		}
	case "get":
		if len(args) != 1 {
			log.Fatal("usage: trendyol get <url-or-id> (NETWORK COMMAND)")
		}
		mustNetwork(cfg)
		_, factory := networkFactory(cfg)
		session, err := factory.NewSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
		candidate, ok := trend.ProductCandidate(args[0])
		if !ok {
			if _, err := strconv.ParseInt(args[0], 10, 64); err != nil {
				log.Fatal("invalid Trendyol URL or product id")
			}
			candidate = store.Candidate{ProductID: args[0], Path: "/-p-" + args[0], URL: "https://www.trendyol.com/-p-" + args[0]}
		}
		result := worker.ScrapeDirect(ctx, session, model.Job{ProductID: candidate.ProductID, Path: candidate.Path, URL: candidate.URL, QueueName: "direct"}, worker.Settings{PricePolicy: trend.PricePolicy{MinUSD: cfg.MinSellingPriceUSD, TRYPerUSD: cfg.TRYPerUSD}, Excluded: cfg.ExcludedCategoryTerms, CloudBypassEnabled: cfg.CloudBypassEnabled, CloudBypassMaxAttempts: cfg.CloudBypassMaxAttempts})
		raw, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(raw))
	case "probe-url":
		if len(args) != 1 || (!strings.HasPrefix(args[0], "https://") && !strings.HasPrefix(args[0], "http://")) {
			log.Fatal("usage: trendyol probe-url <http(s)-url>")
		}
		mustNetwork(cfg)
		_, factory := networkFactory(cfg)
		session, err := factory.NewSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
		response, err := session.Get(ctx, args[0])
		if err != nil {
			log.Fatal(err)
		}
		sample := response.Body
		if len(sample) > 4000 {
			sample = sample[:4000]
		}
		fmt.Printf("status=%d bytes=%d final_url=%s proxy=%s\n%s\n", response.Status, response.Bytes, response.URL, response.ProxyKey, sample)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", command)
		usage()
		os.Exit(2)
	}
}

func mustNetwork(cfg *config.Config) {
	if err := cfg.ValidateRun(); err != nil {
		log.Fatal(err)
	}
}
func networkFactory(cfg *config.Config) (*proxy.Pool, *client.Factory) {
	pool := proxy.New(cfg.ProxyURLs, cfg.ProxyTemplate)
	return pool, client.NewFactory(pool, cfg.RequestTimeout, cfg.StickyPages, cfg.StickyPagesJitter)
}
func newDiscoverer(cfg *config.Config, db *store.DB, session *client.Session, wake chan struct{}) *trend.Discoverer {
	return &trend.Discoverer{Store: db, Session: session, PendingLow: cfg.PendingLow, PendingTarget: cfg.PendingTarget, FreshPendingLow: cfg.FreshPendingLow, FreshPendingTarget: cfg.FreshPendingTarget, ProductTarget: cfg.ProductTarget, SitemapMax: cfg.SitemapMax, Wake: wake}
}

func runMonitor(ctx context.Context, db *store.DB, cfg *config.Config, interval time.Duration, once bool) error {
	opts := monitor.Options{
		ProductTarget:      int64(cfg.ProductTarget),
		PendingLow:         cfg.PendingLow,
		PendingTarget:      cfg.PendingTarget,
		FreshPendingLow:    cfg.FreshPendingLow,
		FreshPendingTarget: cfg.FreshPendingTarget,
		ConfiguredProxies:  configuredProxyCount(cfg),
		CloudBypassEnabled: cfg.CloudBypassEnabled,
		ProxyBudgetGB:      cfg.ProxyTrafficBudgetGB,
		ProxyStopReserveGB: cfg.ProxyStopReserveGB,
	}
	for {
		status, err := db.MonitorSnapshot(ctx)
		if err != nil {
			return err
		}
		if !once {
			fmt.Print("\033[2J\033[H")
		}
		fmt.Print(monitor.Render(status, opts, time.Now()))
		if once {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func parseMonitorArgs(args []string) (time.Duration, bool, error) {
	interval, once := 2*time.Second, false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch {
		case arg == "--once":
			once = true
		case arg == "--interval":
			if index+1 >= len(args) {
				return 0, false, fmt.Errorf("--interval requires a duration, for example 2s")
			}
			index++
			value, err := time.ParseDuration(args[index])
			if err != nil {
				return 0, false, fmt.Errorf("invalid monitor interval %q: %w", args[index], err)
			}
			interval = value
		case strings.HasPrefix(arg, "--interval="):
			value, err := time.ParseDuration(strings.TrimPrefix(arg, "--interval="))
			if err != nil {
				return 0, false, fmt.Errorf("invalid monitor interval %q: %w", arg, err)
			}
			interval = value
		default:
			return 0, false, fmt.Errorf("unknown monitor option %q", arg)
		}
	}
	if interval < time.Second {
		return 0, false, fmt.Errorf("monitor interval must be at least 1s")
	}
	return interval, once, nil
}

func configuredProxyCount(cfg *config.Config) int64 {
	configured := int64(len(cfg.ProxyURLs))
	if cfg.ProxyTemplate != "" {
		configured++
	}
	return configured
}

func printStatus(status store.Counts, cfg *config.Config) {
	monitor.EnrichProxyBudget(&status, cfg.ProxyTrafficBudgetGB, cfg.ProxyStopReserveGB)
	fmt.Println("trendyol status")
	for _, queue := range []string{"direct", "cloudbypass"} {
		q := status.Queues[queue]
		fmt.Printf("  %s pending=%d processing=%d retry=%d done=%d skip=%d\n", queue, q["pending"], q["processing"], q["retry"], q["done"], q["skip"])
	}
	fmt.Printf("  tables products=%d variants=%d sellers=%d offers=%d reviews=%d candidate_sources=%d\n", status.Tables["products"], status.Tables["variants"], status.Tables["sellers"], status.Tables["offers"], status.Tables["reviews"], status.Tables["candidate_sources"])
	fmt.Printf("  product_goal qualified=%d target=%d progress=%.2f%% variants=%d\n", status.Goal.QualifiedProducts, status.Goal.ProductTarget, status.Goal.ProgressPct, status.Goal.VariantRows)
	success := status.Minute[string(model.ResultDone)]
	failures := status.Minute[string(model.ResultRetry)] + status.Minute[string(model.ResultSkip)] + status.Minute[string(model.ResultParserFailure)]
	fmt.Printf("  last_minute success=%g failures=%g response_bytes=%.0f\n", success, failures, status.Minute["response_bytes"])
	fmt.Printf("  traffic last_minute_gb=%.6f total_gb=%.3f projected_target_gb=%.2f projected_remaining_gb=%.2f\n", status.Traffic.LastMinuteGB, status.Traffic.TotalResponseGB, status.Traffic.ProjectedTargetGB, status.Traffic.ProjectedRemainingGB)
	if status.Traffic.BudgetGB > 0 {
		fmt.Printf("  proxy_budget budget_gb=%.3f remaining_gb=%.3f stop_threshold_gb=%.3f reserve_gb=%.3f stop_triggered=%v\n", status.Traffic.BudgetGB, status.Traffic.RemainingGB, status.Traffic.StopThresholdGB, status.Traffic.StopReserveGB, status.Traffic.StopTriggered)
	} else {
		fmt.Println("  proxy_budget disabled (set PROXY_TRAFFIC_BUDGET_GB)")
	}
	fmt.Printf("  ingest products_per_minute=%d variants_per_minute=%d candidates_per_minute=%d\n", status.Ingest.ProductsPerMinute, status.Ingest.VariantsPerMinute, status.Ingest.CandidatesPerMinute)
	fmt.Printf("  http 403=%d 429=%d 503=%d 404=%d network=%g parse=%g\n", status.HTTP["403"], status.HTTP["429"], status.HTTP["503"], status.HTTP["404"], status.Minute["reason:network_error"], status.Minute[string(model.ResultParserFailure)])
	proxyAttempts := status.Proxy.Successes + status.Proxy.Failures
	proxyRate := float64(0)
	if proxyAttempts > 0 {
		proxyRate = float64(status.Proxy.Successes) * 100 / float64(proxyAttempts)
	}
	configuredProxies := configuredProxyCount(cfg)
	if configuredProxies < status.Proxy.Total {
		configuredProxies = status.Proxy.Total
	}
	fmt.Printf("  proxy total=%d cooling=%d success_rate=%.1f%%\n", configuredProxies, status.Proxy.Cooling, proxyRate)
	fmt.Printf("  cloudbypass attempts=%d success=%d failed=%d cost=%.2f budget=%.2f\n", status.Bypass.Attempts, status.Bypass.Successes, status.Bypass.Failures, status.Bypass.Cost, cfg.CloudBypassBudget)
	fmt.Printf("  quality title=%.1f%% brand=%.1f%% price=%.1f%% main_image=%.1f%% category=%.1f%% variation=%.1f%%\n", status.Quality["title_pct"], status.Quality["brand_pct"], status.Quality["price_pct"], status.Quality["main_image_pct"], status.Quality["category_pct"], status.Quality["variation_complete_pct"])
	fmt.Printf("  discovery sitemap=%s offset=%s\n", status.Discovery["discovery_sitemap_index"], status.Discovery["discovery_sitemap_offset"])
	fmt.Printf("  discovery_refill sitemap_per_minute=%d public_page_per_minute=%d\n", status.Ingest.SitemapCandidatesPerMinute, status.Ingest.PublicCandidatesPerMinute)
	fmt.Printf("  candidate_priority high=%s neutral=%s low=%s\n", status.Discovery["priority_high"], status.Discovery["priority_neutral"], status.Discovery["priority_low"])
}

func usage() {
	fmt.Print(strings.TrimSpace(`trendyol — production Trendyol crawler (Go + SQLite WAL + tls-client)

Commands:
  trendyol init                         initialize/migrate SQLite without discarding Python state
  trendyol status [--json]              queue, data, proxy, bypass, quality and discovery metrics
  trendyol monitor [--interval 2s]      live 1M-product dashboard (read-only; --once for one frame)
  trendyol discover                     NETWORK: run one low-watermark discovery refill
  trendyol import-urls <file>            offline import product/listing/search URLs
  trendyol run                          NETWORK: progressive discovery + dispatcher/workers/writer
  trendyol get <url-or-id>              NETWORK: fetch and parse one product for manual diagnosis
  trendyol probe-url <url>               NETWORK: fetch one URL and print a bounded response sample
  trendyol recover                      recover expired processing leases
  trendyol recover-worker <worker-id>   recover one stopped dispatcher's claims
  trendyol reprioritize                 offline score existing queue by high-value category hints
  trendyol prepare-usd                  offline requeue old TRY-filtered/non-USD product jobs
  trendyol prepare-content-backfill     offline requeue accepted products with empty detail text
  trendyol reset-candidates             DESTRUCTIVE: clear candidate queue/discovery state only
  trendyol normalize-data               offline normalize persisted SKU parent relationships
  trendyol export [file.csv]            WAL snapshot -> validated atomic 30-column CSV + manifest
  trendyol validate [file.csv]          stream validate CSV and print bytes/SHA-256
  trendyol help                         show this help

Network commands never run as part of go test. CloudBypass is disabled unless
CLOUDBYPASS_ENABLED=true and uses only the explicit challenge queue.`) + "\n")
}
