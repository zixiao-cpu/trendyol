package worker

import (
	"context"

	"trendyol/internal/config"
	"trendyol/internal/model"
	"trendyol/internal/store"
)

type Writer struct {
	Store                *store.DB
	Config               *config.Config
	DirectWake           chan<- struct{}
	BypassWake           chan<- struct{}
	InitialResponseBytes int64
	StopAtResponseBytes  int64
	BudgetReached        chan<- struct{}
}

func (w *Writer) Run(ctx context.Context, input <-chan model.ScrapeResult) error {
	responseBytes := w.InitialResponseBytes
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-input:
			if !ok {
				return nil
			}
			if err := w.Store.ApplyResult(ctx, result, w.Config.ProductTarget, w.Config.CategoryProductCap, w.Config.MaxAttempts, w.Config.RetryBase, w.Config.FixtureDir); err != nil {
				return err
			}
			responseBytes += maxInt64(result.ResponseBytes, 0)
			if w.StopAtResponseBytes > 0 && responseBytes >= w.StopAtResponseBytes {
				signalWake(w.BudgetReached)
			}
			// Result transitions can create a direct retry or a bypass job while
			// its dispatcher is sleeping with no previously known deadline.
			// Non-blocking signals make both dispatchers recompute availability.
			signalWake(w.DirectWake)
			signalWake(w.BypassWake)
		}
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func signalWake(wake chan<- struct{}) {
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}
