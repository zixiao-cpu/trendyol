package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"trendyol/internal/bypass"
	"trendyol/internal/client"
	"trendyol/internal/config"
	"trendyol/internal/model"
	"trendyol/internal/store"
	trend "trendyol/internal/trendyol"
)

type BackgroundDiscoverer interface {
	Run(context.Context) error
}

func Run(ctx context.Context, db *store.DB, cfg *config.Config, factory *client.Factory, bypassClient *bypass.Client, discoverer BackgroundDiscoverer, wake chan struct{}) error {
	initialResponseBytes, err := db.TotalResponseBytes(ctx)
	if err != nil {
		return err
	}
	stopAtResponseBytes := int64(0)
	if cfg.ProxyTrafficBudgetGB > 0 {
		stopAtResponseBytes = int64((cfg.ProxyTrafficBudgetGB - cfg.ProxyStopReserveGB) * 1_000_000_000)
		if initialResponseBytes >= stopAtResponseBytes {
			fmt.Printf("proxy traffic stop threshold already reached: used=%.6f GB threshold=%.6f GB; no requests started\n",
				float64(initialResponseBytes)/1_000_000_000, float64(stopAtResponseBytes)/1_000_000_000)
			return nil
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var contentBackfillOnly atomic.Bool
	backfillActive, err := db.ActiveContentBackfill(runCtx)
	if err != nil {
		return err
	}
	if backfillActive > 0 {
		contentBackfillOnly.Store(true)
		fmt.Printf("content backfill phase started: old_products_remaining=%d; discovery and new candidates paused\n", backfillActive)
	}
	directWake := wake
	if directWake == nil {
		directWake = make(chan struct{}, 1)
	}
	bypassWake := make(chan struct{}, 1)
	if !cfg.CloudBypassEnabled || cfg.BypassConcurrency == 0 {
		if _, err := db.DisableBypassQueue(runCtx); err != nil {
			return err
		}
	}
	directJobs := make(chan model.Job, cfg.ClaimBatch)
	bypassJobs := make(chan model.Job, maxInt(cfg.ClaimBatch/4, cfg.BypassConcurrency*2))
	results := make(chan model.ScrapeResult, cfg.ClaimBatch)
	writerExited := make(chan struct{})
	errors := make(chan error, 4)
	runID := fmt.Sprintf("go-%d-%d", os.Getpid(), time.Now().UnixNano())
	directOwner, bypassOwner := runID+":direct-dispatcher", runID+":bypass-dispatcher"

	var dispatchers sync.WaitGroup
	startDispatcher := func(dispatcher *Dispatcher, output chan<- model.Job) {
		dispatchers.Add(1)
		go func() {
			defer dispatchers.Done()
			if err := dispatcher.Run(runCtx, output); err != nil && runCtx.Err() == nil {
				select {
				case errors <- err:
				default:
				}
			}
		}()
	}
	backfillGate := func() bool { return contentBackfillOnly.Load() }
	startDispatcher(&Dispatcher{Store: db, Queue: "direct", Owner: directOwner, Batch: cfg.ClaimBatch, Lease: cfg.LeaseDuration, Wake: directWake, ContentBackfillOnly: backfillGate}, directJobs)
	if cfg.CloudBypassEnabled && cfg.BypassConcurrency > 0 {
		startDispatcher(&Dispatcher{Store: db, Queue: "cloudbypass", Owner: bypassOwner, Batch: maxInt(1, cfg.BypassConcurrency*2), Lease: cfg.LeaseDuration, Wake: bypassWake, ContentBackfillOnly: backfillGate}, bypassJobs)
	}

	settings := Settings{PricePolicy: trend.PricePolicy{MinUSD: cfg.MinSellingPriceUSD, TRYPerUSD: cfg.TRYPerUSD}, Excluded: cfg.ExcludedCategoryTerms, CloudBypassEnabled: cfg.CloudBypassEnabled, CloudBypassMaxAttempts: cfg.CloudBypassMaxAttempts}
	var workers sync.WaitGroup
	for index := 0; index < cfg.Concurrency; index++ {
		session, err := factory.NewSession()
		if err != nil {
			cancel()
			return err
		}
		workers.Add(1)
		go func(session *client.Session) {
			defer workers.Done()
			defer session.Close()
			for {
				select {
				case <-runCtx.Done():
					return
				case job := <-directJobs:
					result := ScrapeDirect(runCtx, session, job, settings)
					// Even after a budget cancellation, persist the response that was
					// already in flight. Only abandon it if the writer itself failed.
					select {
					case results <- result:
					case <-writerExited:
						return
					}
				}
			}
		}(session)
	}
	if cfg.CloudBypassEnabled && cfg.BypassConcurrency > 0 {
		for index := 0; index < cfg.BypassConcurrency; index++ {
			proxySession, err := factory.NewSession()
			if err != nil {
				cancel()
				return err
			}
			workers.Add(1)
			go func(session *client.Session) {
				defer workers.Done()
				defer session.Close()
				for {
					select {
					case <-runCtx.Done():
						return
					case job := <-bypassJobs:
						result := ScrapeBypass(runCtx, bypassClient, session, job, settings)
						select {
						case results <- result:
						case <-writerExited:
							return
						}
					}
				}
			}(proxySession)
		}
	}

	writerDone := make(chan error, 1)
	budgetReached := make(chan struct{}, 1)
	go func() {
		defer close(writerExited)
		err := (&Writer{Store: db, Config: cfg, DirectWake: directWake, BypassWake: bypassWake,
			InitialResponseBytes: initialResponseBytes, StopAtResponseBytes: stopAtResponseBytes, BudgetReached: budgetReached}).Run(context.Background(), results)
		writerDone <- err
		if err != nil {
			select {
			case errors <- err:
			default:
			}
		}
	}()
	discovererStarted := false
	startDiscoverer := func() {
		if discoverer == nil || discovererStarted {
			return
		}
		discovererStarted = true
		go func() {
			if err := discoverer.Run(runCtx); err != nil && runCtx.Err() == nil {
				select {
				case errors <- err:
				default:
				}
			}
		}()
	}
	if !contentBackfillOnly.Load() {
		startDiscoverer()
	}

	var runErr error
	ticker := time.NewTicker(time.Second)
	recoveryTicker := time.NewTicker(30 * time.Second)
loop:
	for {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
			break loop
		case err := <-errors:
			runErr = err
			break loop
		case <-budgetReached:
			fmt.Printf("proxy traffic stop threshold reached; stopping cleanly after persisting in-flight results\n")
			break loop
		case <-recoveryTicker.C:
			recovered, err := db.RecoverStale(runCtx)
			if err != nil {
				runErr = err
				break loop
			}
			if recovered > 0 {
				fmt.Printf("periodic stale-lease recovery: jobs=%d\n", recovered)
				signalWake(directWake)
				signalWake(bypassWake)
			}
		case <-ticker.C:
			if contentBackfillOnly.Load() {
				remaining, err := db.ActiveContentBackfill(runCtx)
				if err != nil {
					runErr = err
					break loop
				}
				if remaining == 0 {
					contentBackfillOnly.Store(false)
					fmt.Println("content backfill phase complete; enabling discovery and new-product candidates")
					signalWake(directWake)
					signalWake(bypassWake)
					startDiscoverer()
				}
			}
			if cfg.ProxyTrafficBudgetGB > 0 {
				usedBytes, err := db.TotalResponseBytes(runCtx)
				if err != nil {
					runErr = err
					break loop
				}
				if usedBytes >= stopAtResponseBytes {
					fmt.Printf("proxy traffic stop threshold reached: used=%.6f GB threshold=%.6f GB budget=%.6f GB reserve=%.6f GB; stopping cleanly\n",
						float64(usedBytes)/1_000_000_000, float64(stopAtResponseBytes)/1_000_000_000, cfg.ProxyTrafficBudgetGB, cfg.ProxyStopReserveGB)
					break loop
				}
			}
			count, err := db.ProductCount(runCtx)
			if err != nil {
				runErr = err
				break loop
			}
			if !contentBackfillOnly.Load() && cfg.ProductTarget > 0 && int(count) >= cfg.ProductTarget {
				break loop
			}
		}
	}
	ticker.Stop()
	recoveryTicker.Stop()
	cancel()
	dispatchers.Wait()
	workers.Wait()
	close(results)
	if err := <-writerDone; err != nil && runErr == nil {
		runErr = err
	}
	_, _ = db.RecoverWorker(context.Background(), directOwner)
	_, _ = db.RecoverWorker(context.Background(), bypassOwner)
	if runErr == context.Canceled {
		return nil
	}
	return runErr
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
