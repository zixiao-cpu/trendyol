package worker

import (
	"context"
	"time"

	"trendyol/internal/model"
	"trendyol/internal/store"
)

type Dispatcher struct {
	Store *store.DB
	Queue string
	Owner string
	Batch int
	Lease time.Duration
	Wake  <-chan struct{}
	// ContentBackfillOnly is evaluated before every claim/wait so the worker
	// pool can switch phases without being restarted.
	ContentBackfillOnly func() bool
}

func (d *Dispatcher) Run(ctx context.Context, output chan<- model.Job) error {
	for {
		backfillOnly := d.ContentBackfillOnly != nil && d.ContentBackfillOnly()
		var jobs []model.Job
		var err error
		if backfillOnly {
			jobs, err = d.Store.BatchClaimContentBackfill(ctx, d.Queue, d.Owner, d.Batch, d.Lease)
		} else {
			jobs, err = d.Store.BatchClaim(ctx, d.Queue, d.Owner, d.Batch, d.Lease)
		}
		if err != nil {
			return err
		}
		if len(jobs) > 0 {
			for _, job := range jobs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case output <- job:
				}
			}
			continue
		}
		var next time.Time
		var ok bool
		if backfillOnly {
			next, ok, err = d.Store.NextContentBackfillAvailable(ctx, d.Queue)
		} else {
			next, ok, err = d.Store.NextAvailable(ctx, d.Queue)
		}
		if err != nil {
			return err
		}
		// A wake signal interrupts this wait whenever discovery or the writer
		// makes work immediately available. With a known next_available_at there
		// is no need to poll SQLite on an arbitrary 30-second cadence.
		wait := 10 * time.Minute
		if ok {
			wait = time.Until(next)
			if wait < 10*time.Millisecond {
				wait = 10 * time.Millisecond
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-d.Wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}
