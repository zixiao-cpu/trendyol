package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"trendyol/internal/model"
)

type DB struct {
	sql *sql.DB
}

type Candidate struct {
	ProductID string
	Path      string
	URL       string
	Source    string
	SourceRef string
	Priority  int
}

type Counts struct {
	Queues    map[string]map[string]int64 `json:"queues"`
	Tables    map[string]int64            `json:"tables"`
	Minute    map[string]float64          `json:"last_minute"`
	HTTP      map[string]int64            `json:"http_failures"`
	Proxy     ProxySummary                `json:"proxy"`
	Bypass    BypassSummary               `json:"cloudbypass"`
	Goal      GoalSummary                 `json:"product_goal"`
	Traffic   TrafficSummary              `json:"proxy_traffic"`
	Ingest    IngestSummary               `json:"ingest"`
	Quality   map[string]float64          `json:"quality"`
	Discovery map[string]string           `json:"discovery"`
	Backfill  BackfillSummary             `json:"content_backfill"`
}

type ProxySummary struct{ Total, Cooling, Successes, Failures int64 }
type BypassSummary struct {
	Attempts, Successes, Failures int64
	Cost                          float64
}
type GoalSummary struct {
	QualifiedProducts int64   `json:"qualified_products"`
	ProductTarget     int64   `json:"product_target"`
	VariantRows       int64   `json:"variant_rows"`
	ProgressPct       float64 `json:"progress_pct"`
}
type TrafficSummary struct {
	LastMinuteBytes        int64   `json:"last_minute_bytes"`
	LastMinuteGB           float64 `json:"last_minute_gb"`
	TotalResponseBytes     int64   `json:"total_response_bytes"`
	TotalResponseGB        float64 `json:"total_response_gb"`
	AverageBytesPerProduct float64 `json:"average_bytes_per_product"`
	ProjectedTargetGB      float64 `json:"projected_target_gb"`
	ProjectedRemainingGB   float64 `json:"projected_remaining_gb"`
	ProjectionProducts     int64   `json:"projection_products"`
	BudgetGB               float64 `json:"budget_gb"`
	StopReserveGB          float64 `json:"stop_reserve_gb"`
	StopThresholdGB        float64 `json:"stop_threshold_gb"`
	RemainingGB            float64 `json:"remaining_gb"`
	StopTriggered          bool    `json:"stop_triggered"`
}
type IngestSummary struct {
	ProductsPerMinute          int64   `json:"products_per_minute"`
	VariantsPerMinute          int64   `json:"variants_per_minute"`
	AttemptsPerMinute          int64   `json:"attempts_per_minute"`
	FailuresPerMinute          int64   `json:"failures_per_minute"`
	CandidatesPerMinute        int64   `json:"candidates_per_minute"`
	SitemapCandidatesPerMinute int64   `json:"sitemap_candidates_per_minute"`
	PublicCandidatesPerMinute  int64   `json:"public_page_candidates_per_minute"`
	ETASeconds                 float64 `json:"eta_seconds"`
	SuccessesPerMinute         int64   `json:"successes_per_minute"`
	SuccessRatePct             float64 `json:"success_rate_pct"`
	CumulativeSuccesses        int64   `json:"cumulative_successes"`
	CumulativeAttempts         int64   `json:"cumulative_attempts"`
	CumulativeSuccessRatePct   float64 `json:"cumulative_success_rate_pct"`
}
type BackfillSummary struct {
	TotalProducts   int64 `json:"total_products"`
	FilledProducts  int64 `json:"filled_products"`
	ActiveJobs      int64 `json:"active_jobs"`
	TerminalMissing int64 `json:"terminal_missing"`
}

const (
	MetricDiscoverySitemap    = "discovery:sitemap"
	MetricDiscoveryPublicPage = "discovery:public_page"
	ContentBackfillPriority   = 400
)

func Open(path string) (*DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)&_txlock=immediate", filepath.ToSlash(path))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	if err := migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{sql: db}, nil
}

func (d *DB) Close() error { return d.sql.Close() }
func (d *DB) SQL() *sql.DB { return d.sql }

// TotalResponseBytes returns the durable count of HTTP response-body bytes
// recorded for product, retry and discovery requests. It is intentionally read
// from metric_totals so budget enforcement survives process and host restarts.
func (d *DB) TotalResponseBytes(ctx context.Context) (int64, error) {
	var total float64
	err := d.sql.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key='response_bytes'),0)`).Scan(&total)
	return int64(math.Round(total)), err
}

func (d *DB) Enqueue(ctx context.Context, candidates []Candidate) (int64, error) {
	if len(candidates) == 0 {
		return 0, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	var added int64
	for _, c := range candidates {
		source := strings.TrimSpace(c.Source)
		if source == "" {
			source = "unknown"
		}
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO jobs(product_id,path,url,state,queue_name,priority,available_at,discovered_at,updated_at)
VALUES(?,?,?,'pending','direct',?,0,?,?)`, c.ProductID, c.Path, c.URL, c.Priority, now, now)
		if err != nil {
			return added, err
		}
		n, _ := res.RowsAffected()
		added += n
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET priority=MAX(priority,?) WHERE product_id=?`, c.Priority, c.ProductID); err != nil {
			return added, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO candidate_sources(product_id,source,source_ref,sightings,first_seen_at,last_seen_at)
VALUES(?,?,?,1,?,?) ON CONFLICT(product_id,source) DO UPDATE SET
source_ref=CASE WHEN excluded.source_ref<>'' THEN excluded.source_ref ELSE candidate_sources.source_ref END,
sightings=candidate_sources.sightings+1,last_seen_at=excluded.last_seen_at`, c.ProductID, source, c.SourceRef, now, now); err != nil {
			return added, err
		}
	}
	return added, tx.Commit()
}

func (d *DB) BatchClaim(ctx context.Context, queue, owner string, limit int, lease time.Duration) ([]model.Job, error) {
	return d.batchClaim(ctx, queue, owner, limit, lease, false)
}

// BatchClaimContentBackfill only claims the administrative priority band used
// by PrepareContentBackfill. Normal Sitemap/listing candidates are therefore
// unable to leak into the old-data repair phase while retries are delayed.
func (d *DB) BatchClaimContentBackfill(ctx context.Context, queue, owner string, limit int, lease time.Duration) ([]model.Job, error) {
	return d.batchClaim(ctx, queue, owner, limit, lease, true)
}

func (d *DB) batchClaim(ctx context.Context, queue, owner string, limit int, lease time.Duration, contentBackfillOnly bool) ([]model.Job, error) {
	if limit < 1 {
		return nil, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now()
	query := `SELECT id,product_id,path,url,state,queue_name,attempts,quality_attempts,bypass_attempts
FROM jobs WHERE queue_name=? AND state IN ('pending','retry') AND available_at<=?`
	args := []any{queue, float64(now.UnixNano()) / 1e9}
	if contentBackfillOnly {
		query += ` AND priority>=? AND EXISTS (
SELECT 1 FROM products p WHERE p.product_id=jobs.product_id AND TRIM(p.description)='')`
		args = append(args, ContentBackfillPriority)
	}
	query += ` ORDER BY priority DESC,available_at,id LIMIT ?`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var jobs []model.Job
	for rows.Next() {
		var j model.Job
		if err := rows.Scan(&j.ID, &j.ProductID, &j.Path, &j.URL, &j.State, &j.QueueName, &j.Attempts, &j.QualityAttempts, &j.BypassAttempts); err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	claimed := jobs[:0]
	leaseUntil := float64(now.Add(lease).UnixNano()) / 1e9
	for _, j := range jobs {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET state='processing',attempts=attempts+1,
bypass_attempts=bypass_attempts+CASE WHEN queue_name='cloudbypass' THEN 1 ELSE 0 END,
lease_until=?,worker_id=?,updated_at=? WHERE id=? AND state IN ('pending','retry')`, leaseUntil, owner, now.UTC().Format(time.RFC3339), j.ID)
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			from := j.State
			j.State = "processing"
			j.WorkerID = owner
			j.Attempts++
			if queue == "cloudbypass" {
				j.BypassAttempts++
			}
			j.LeaseUntil = now.Add(lease)
			claimed = append(claimed, j)
			if err := insertEvent(ctx, tx, j.ID, from, "processing", queue, owner); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (d *DB) NextAvailable(ctx context.Context, queue string) (time.Time, bool, error) {
	return d.nextAvailable(ctx, queue, false)
}

func (d *DB) NextContentBackfillAvailable(ctx context.Context, queue string) (time.Time, bool, error) {
	return d.nextAvailable(ctx, queue, true)
}

func (d *DB) nextAvailable(ctx context.Context, queue string, contentBackfillOnly bool) (time.Time, bool, error) {
	var epoch sql.NullFloat64
	query := `SELECT MIN(available_at) FROM jobs WHERE queue_name=? AND state IN ('pending','retry')`
	if contentBackfillOnly {
		query += ` AND priority>=? AND EXISTS (
SELECT 1 FROM products p WHERE p.product_id=jobs.product_id AND TRIM(p.description)='')`
	}
	args := []any{queue}
	if contentBackfillOnly {
		args = append(args, ContentBackfillPriority)
	}
	err := d.sql.QueryRowContext(ctx, query, args...).Scan(&epoch)
	if err != nil || !epoch.Valid {
		return time.Time{}, false, err
	}
	sec, frac := math.Modf(epoch.Float64)
	return time.Unix(int64(sec), int64(frac*1e9)), true, nil
}

func (d *DB) ActiveContentBackfill(ctx context.Context) (int64, error) {
	var count int64
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs j
WHERE j.priority>=? AND j.state IN ('pending','processing','retry')
AND EXISTS (SELECT 1 FROM products p WHERE p.product_id=j.product_id AND TRIM(p.description)='')`, ContentBackfillPriority).Scan(&count)
	return count, err
}

func (d *DB) loadBackfillSummary(ctx context.Context, out *Counts) {
	_ = d.sql.QueryRowContext(ctx, `SELECT
COUNT(*),
COALESCE(SUM(CASE WHEN TRIM(p.description)<>'' THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN j.state IN ('pending','processing','retry') THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN j.state IN ('done','skip') AND TRIM(p.description)='' THEN 1 ELSE 0 END),0)
FROM jobs j JOIN products p ON p.product_id=j.product_id WHERE j.priority>=?`, ContentBackfillPriority).Scan(
		&out.Backfill.TotalProducts, &out.Backfill.FilledProducts, &out.Backfill.ActiveJobs, &out.Backfill.TerminalMissing)
}

func (d *DB) RecoverStale(ctx context.Context) (int64, error) {
	now := time.Now()
	rows, err := d.sql.QueryContext(ctx, `SELECT id,queue_name FROM jobs WHERE state='processing' AND (lease_until IS NULL OR lease_until<?)`, float64(now.UnixNano())/1e9)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ids []int64
	var queues []string
	for rows.Next() {
		var id int64
		var q string
		if err := rows.Scan(&id, &q); err != nil {
			return 0, err
		}
		ids = append(ids, id)
		queues = append(queues, q)
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for i, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state='retry',available_at=?,lease_until=NULL,worker_id=NULL,last_error='stale_lease_recovered',updated_at=? WHERE id=?`, float64(now.UnixNano())/1e9, now.UTC().Format(time.RFC3339), id); err != nil {
			return 0, err
		}
		if err := insertEvent(ctx, tx, id, "processing", "retry", queues[i], "stale_lease_recovered"); err != nil {
			return 0, err
		}
	}
	return int64(len(ids)), tx.Commit()
}

func (d *DB) RecoverWorker(ctx context.Context, workerID string) (int64, error) {
	if strings.TrimSpace(workerID) == "" {
		return 0, fmt.Errorf("empty worker id")
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,queue_name FROM jobs WHERE state='processing' AND worker_id=?`, workerID)
	if err != nil {
		return 0, err
	}
	type claimedJob struct {
		id    int64
		queue string
	}
	var jobs []claimedJob
	for rows.Next() {
		var job claimedJob
		if err := rows.Scan(&job.id, &job.queue); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	now := time.Now()
	for _, job := range jobs {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET state='retry',available_at=?,lease_until=NULL,worker_id=NULL,last_error='worker_recovered',updated_at=? WHERE id=? AND state='processing' AND worker_id=?`, float64(now.UnixNano())/1e9, now.UTC().Format(time.RFC3339), job.id, workerID)
		if err != nil {
			return 0, err
		}
		if changed, _ := res.RowsAffected(); changed == 1 {
			if err := insertEvent(ctx, tx, job.id, "processing", "retry", job.queue, "worker_recovered"); err != nil {
				return 0, err
			}
		}
	}
	return int64(len(jobs)), tx.Commit()
}

// PrepareUSDReprice removes any previously persisted non-USD product bundles
// and requeues both those jobs and products skipped by the former TRY threshold.
// It is an explicit one-time upgrade action, not part of normal startup.
func (d *DB) PrepareUSDReprice(ctx context.Context) (requeued, removed int64, err error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET state='pending',queue_name='direct',attempts=0,quality_attempts=0,
bypass_attempts=0,available_at=0,lease_until=NULL,worker_id=NULL,last_error=NULL,http_status=NULL,updated_at=?
WHERE product_id IN (SELECT DISTINCT product_id FROM variants WHERE currency<>'USD')`, now)
	if err != nil {
		return 0, 0, err
	}
	nonUSD, _ := result.RowsAffected()
	result, err = tx.ExecContext(ctx, `DELETE FROM products WHERE product_id IN (SELECT DISTINCT product_id FROM variants WHERE currency<>'USD')`)
	if err != nil {
		return 0, 0, err
	}
	removed, _ = result.RowsAffected()
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET state='pending',queue_name='direct',attempts=0,quality_attempts=0,
bypass_attempts=0,available_at=0,lease_until=NULL,worker_id=NULL,last_error=NULL,http_status=NULL,updated_at=?
WHERE state='skip' AND last_error='no_eligible_variants'`, now)
	if err != nil {
		return 0, 0, err
	}
	priceFiltered, _ := result.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return nonUSD + priceFiltered, removed, nil
}

// PrepareContentBackfill requeues accepted products whose legacy records have
// no detail-page text. Existing commerce rows remain authoritative until a
// successful refresh replaces them, so a transient backfill failure cannot
// erase a previously accepted product or its variants.
func (d *DB) PrepareContentBackfill(ctx context.Context) (int64, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	insertedResult, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO jobs(
product_id,path,url,state,queue_name,priority,available_at,discovered_at,updated_at)
SELECT p.product_id,REPLACE(p.url,'https://www.trendyol.com',''),p.url,'pending','direct',400,0,?,?
FROM products p WHERE TRIM(p.description)=''`, now, now)
	if err != nil {
		return 0, err
	}
	inserted, _ := insertedResult.RowsAffected()
	if _, err := tx.ExecContext(ctx, `INSERT INTO job_events(job_id,from_state,to_state,queue_name,reason,created_at)
SELECT j.id,j.state,'pending','direct','content_backfill',?
FROM jobs j JOIN products p ON p.product_id=j.product_id
WHERE TRIM(p.description)=''
AND NOT EXISTS(SELECT 1 FROM job_events e WHERE e.job_id=j.id AND e.reason='content_backfill')`, now); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET state='pending',queue_name='direct',priority=MAX(priority,400),
attempts=0,quality_attempts=0,bypass_attempts=0,available_at=0,lease_until=NULL,worker_id=NULL,
last_error=NULL,http_status=NULL,updated_at=?
WHERE product_id IN (SELECT product_id FROM products WHERE TRIM(description)='')
AND (state<>'pending' OR queue_name<>'direct' OR priority<400 OR attempts<>0 OR quality_attempts<>0 OR
bypass_attempts<>0 OR available_at<>0 OR lease_until IS NOT NULL OR worker_id IS NOT NULL OR
last_error IS NOT NULL OR http_status IS NOT NULL)`, now)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return inserted + count, nil
}

// ResetCandidates removes discovery/queue state while preserving all already
// accepted commerce tables. It is intentionally explicit because it discards
// pending, retry, done and skipped job history.
func (d *DB) ResetCandidates(ctx context.Context) (int64, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&count); err != nil {
		return 0, err
	}
	for _, statement := range []string{
		`DELETE FROM response_failures`,
		`DELETE FROM job_events`,
		`DELETE FROM candidate_sources`,
		`DELETE FROM jobs`,
		`DELETE FROM seed_pages`,
		`DELETE FROM metadata WHERE key LIKE 'discovery_%'`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return 0, err
		}
	}
	return count, tx.Commit()
}

// NormalizeStandaloneParents keeps the persisted SKU relationship consistent:
// a row without an explicit variation pair is a standalone SKU and therefore
// uses its own product as parent rather than an unrelated Trendyol group id.
func (d *DB) NormalizeStandaloneParents(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := d.sql.ExecContext(ctx, `UPDATE variants SET product_group_id=product_id,updated_at=?
WHERE variation_theme='' AND product_group_id<>product_id`, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *DB) Pending(ctx context.Context, queue string) (int64, error) {
	var n int64
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE queue_name=? AND state IN ('pending','retry','processing')`, queue).Scan(&n)
	return n, err
}

func (d *DB) PendingAtLeastPriority(ctx context.Context, queue string, priority int) (int64, error) {
	var count int64
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE queue_name=? AND state IN ('pending','retry') AND priority>=?`, queue, priority).Scan(&count)
	return count, err
}

func (d *DB) Active(ctx context.Context) (int64, error) {
	var n int64
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE state IN ('pending','retry','processing')`).Scan(&n)
	return n, err
}
func (d *DB) VariantCount(ctx context.Context) (int64, error) {
	var n int64
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM variants`).Scan(&n)
	return n, err
}

func (d *DB) ProductCount(ctx context.Context) (int64, error) {
	var n int64
	err := d.sql.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM metadata WHERE key='accepted_product_count'`).Scan(&n)
	if err != sql.ErrNoRows {
		return n, err
	}
	if err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM products`).Scan(&n); err != nil {
		return 0, err
	}
	_, err = d.sql.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('accepted_product_count',?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(n, 10))
	return n, err
}

func (d *DB) BypassSpent(ctx context.Context) (float64, error) {
	var spent float64
	err := d.sql.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost),0) FROM bypass_usage`).Scan(&spent)
	return spent, err
}

// RecordDiscovery records one proxy-backed discovery response together with
// the number of new product candidates it added. Keeping both values on one
// event lets the rolling monitor report refill throughput without counting a
// second synthetic event as another HTTP response.
func (d *DB) RecordDiscovery(ctx context.Context, kind string, added int64, httpStatus int, responseBytes int64) error {
	if kind != MetricDiscoverySitemap && kind != MetricDiscoveryPublicPage {
		return fmt.Errorf("invalid discovery metric kind %q", kind)
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := recordResponseMetric(ctx, tx, kind, float64(added), httpStatus, "discovery", responseBytes, time.Now()); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) ApplyResult(ctx context.Context, result model.ScrapeResult, productTarget, categoryProductCap, maxAttempts int, retryBase time.Duration, fixtureDir string) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now()
	toState, queue, reason := "skip", result.Job.QueueName, result.Reason
	switch result.Kind {
	case model.ResultDone:
		if result.Bundle == nil {
			return fmt.Errorf("done result without bundle")
		}
		saved, capacityReason, err := saveBundle(ctx, tx, result.Bundle, productTarget, categoryProductCap)
		if err != nil {
			return err
		}
		if saved {
			toState = "done"
			reason = ""
		} else {
			result.Kind = model.ResultSkip
			toState = "skip"
			reason = capacityReason
		}
	case model.ResultChallenge:
		toState = "pending"
		queue = "cloudbypass"
	case model.ResultRetry, model.ResultParserFailure, model.ResultQualityRetry:
		if result.Kind == model.ResultQualityRetry || result.Kind == model.ResultParserFailure {
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET quality_attempts=quality_attempts+1 WHERE id=?`, result.Job.ID); err != nil {
				return err
			}
		}
		if result.Job.Attempts >= maxAttempts {
			toState = "skip"
			reason = "retry_exhausted:" + reason
		} else {
			toState = "retry"
			exp := result.Job.Attempts - 1
			if exp > 8 {
				exp = 8
			}
			delay := retryBase * time.Duration(1<<max(exp, 0))
			if delay > 6*time.Hour {
				delay = 6 * time.Hour
			}
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET available_at=? WHERE id=?`, float64(now.Add(delay).UnixNano())/1e9, result.Job.ID); err != nil {
				return err
			}
		}
	case model.ResultSkip:
		toState = "skip"
	}
	if result.Kind == model.ResultParserFailure && len(result.Fixture) > 0 {
		hash := sha256.Sum256(result.Fixture)
		name := hex.EncodeToString(hash[:]) + ".html"
		path := filepath.Join(fixtureDir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
			_ = os.WriteFile(path, result.Fixture, 0o600)
		}
		_, _ = tx.ExecContext(ctx, `INSERT INTO response_failures(job_id,body_sha256,fixture_path,reason,created_at) VALUES(?,?,?,?,?)`, result.Job.ID, hex.EncodeToString(hash[:]), path, reason, now.UTC().Format(time.RFC3339))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?,queue_name=?,lease_until=NULL,worker_id=NULL,last_error=?,http_status=?,updated_at=? WHERE id=?`, toState, queue, reason, result.HTTPStatus, now.UTC().Format(time.RFC3339), result.Job.ID); err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, result.Job.ID, "processing", toState, queue, reason); err != nil {
		return err
	}
	kind := string(result.Kind)
	metricValue := float64(1)
	if result.Kind == model.ResultDone && result.Bundle != nil {
		metricValue = float64(len(result.Bundle.Variants))
	}
	if err := recordResponseMetric(ctx, tx, kind, metricValue, result.HTTPStatus, result.Job.QueueName, result.ResponseBytes, now); err != nil {
		return err
	}
	if reason != "" {
		_, _ = tx.ExecContext(ctx, `INSERT INTO metric_events(kind,value,http_status,queue_name,bytes,created_at) VALUES(?,?,?,?,0,?)`, "reason:"+reason, 1, result.HTTPStatus, result.Job.QueueName, float64(now.UnixNano())/1e9)
	}
	parsedSuccessfully := result.Kind == model.ResultDone || reason == "product_target_reached" || reason == "category_product_cap_reached"
	if result.ProxyKey != "" {
		success := parsedSuccessfully
		if success {
			_, _ = tx.ExecContext(ctx, `INSERT INTO proxy_stats(proxy_key,successes,failures,cooldown_until,updated_at) VALUES(?,1,0,0,?) ON CONFLICT(proxy_key) DO UPDATE SET successes=successes+1,cooldown_until=0,updated_at=excluded.updated_at`, result.ProxyKey, now.UTC().Format(time.RFC3339))
		} else {
			_, _ = tx.ExecContext(ctx, `INSERT INTO proxy_stats(proxy_key,successes,failures,cooldown_until,updated_at) VALUES(?,0,1,?,?) ON CONFLICT(proxy_key) DO UPDATE SET failures=failures+1,cooldown_until=excluded.cooldown_until,updated_at=excluded.updated_at`, result.ProxyKey, float64(now.Add(time.Minute).UnixNano())/1e9, now.UTC().Format(time.RFC3339))
		}
	}
	if result.Job.QueueName == "cloudbypass" && result.BypassAttempted {
		success := 0
		if parsedSuccessfully {
			success = 1
		}
		_, _ = tx.ExecContext(ctx, `INSERT INTO bypass_usage(job_id,success,duration_ms,cost,error,created_at) VALUES(?,?,?,?,?,?)`, result.Job.ID, success, result.Duration.Milliseconds(), result.BypassCost, reason, now.UTC().Format(time.RFC3339))
	}
	return tx.Commit()
}

func saveBundle(ctx context.Context, tx *sql.Tx, b *model.ProductBundle, productTarget, categoryProductCap int) (bool, string, error) {
	if b == nil || len(b.Variants) == 0 {
		return false, "", fmt.Errorf("empty bundle")
	}
	p := b.Product
	now := time.Now().UTC().Format(time.RFC3339)
	var productExists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM products WHERE product_id=?)`, p.ProductID).Scan(&productExists); err != nil {
		return false, "", err
	}
	categoryKey := b.Variants[0].CategoryKey
	if categoryKey == "" {
		categoryKey = p.CategoryPath
	}
	if categoryKey == "" {
		categoryKey = "_uncategorized"
	}
	if productExists == 0 {
		var products int
		if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM metadata WHERE key='accepted_product_count'`).Scan(&products); err != nil {
			return false, "", err
		}
		if productTarget > 0 && products >= productTarget {
			return false, "product_target_reached", nil
		}
		if categoryProductCap > 0 {
			var categoryProducts int
			err := tx.QueryRowContext(ctx, `SELECT product_count FROM category_product_counts WHERE category_key=?`, categoryKey).Scan(&categoryProducts)
			if err != nil && err != sql.ErrNoRows {
				return false, "", err
			}
			if categoryProducts >= categoryProductCap {
				return false, "category_product_cap_reached", nil
			}
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO products(product_id,url,title,brand,main_image,images,description,features,specs,reviews,category_path,category_level_1,category_level_2,category_level_3,category_leaf,category_node_ids,category_source,category_complete,scraped_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'[]',?,?,?,?,?,?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET url=excluded.url,title=excluded.title,brand=excluded.brand,main_image=excluded.main_image,images=excluded.images,description=excluded.description,features=excluded.features,specs=excluded.specs,category_path=excluded.category_path,category_level_1=excluded.category_level_1,category_level_2=excluded.category_level_2,category_level_3=excluded.category_level_3,category_leaf=excluded.category_leaf,category_node_ids=excluded.category_node_ids,category_source=excluded.category_source,category_complete=excluded.category_complete,scraped_at=excluded.scraped_at,updated_at=excluded.updated_at`, p.ProductID, p.URL, p.Title, p.Brand, p.MainImage, p.Images, p.Description, p.Features, p.Specs, p.CategoryPath, p.CategoryLevel1, p.CategoryLevel2, p.CategoryLevel3, p.CategoryLeaf, p.CategoryNodeIDs, p.CategorySource, p.CategoryComplete, p.ScrapedAt, now)
	if err != nil {
		return false, "", err
	}
	variantStatement, err := tx.PrepareContext(ctx, `INSERT INTO variants(item_number,product_id,product_group_id,color,capacity,material,selling_price,currency,variation_theme,variation_value,variation_status,category_key,scraped_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(item_number) DO UPDATE SET product_id=excluded.product_id,product_group_id=excluded.product_group_id,color=excluded.color,capacity=excluded.capacity,material=excluded.material,selling_price=excluded.selling_price,currency=excluded.currency,variation_theme=excluded.variation_theme,variation_value=excluded.variation_value,variation_status=excluded.variation_status,category_key=excluded.category_key,scraped_at=excluded.scraped_at,updated_at=excluded.updated_at`)
	if err != nil {
		return false, "", err
	}
	defer variantStatement.Close()
	for _, v := range b.Variants {
		_, err = variantStatement.ExecContext(ctx, v.ItemNumber, v.ProductID, v.ProductGroupID, v.Color, v.Capacity, v.Material, v.SellingPrice, v.Currency, v.VariationTheme, v.VariationValue, v.VariationStatus, v.CategoryKey, v.ScrapedAt, now)
		if err != nil {
			return false, "", err
		}
	}
	_, _ = tx.ExecContext(ctx, `DELETE FROM reviews WHERE product_id=?`, p.ProductID)
	for i, r := range b.Reviews {
		if i >= 10 {
			break
		}
		raw, _ := json.Marshal(r.Images)
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x1f%s\x1f%s\x1f%s\x1f%s", p.ProductID, r.Body, r.Title, r.Date, r.Author)))
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO reviews(review_key,product_id,stars,title,body,review_date,author,images,position,scraped_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, hex.EncodeToString(sum[:]), p.ProductID, r.Stars, r.Title, r.Body, r.Date, r.Author, string(raw), i, p.ScrapedAt)
		if err != nil {
			return false, "", err
		}
	}
	if b.Seller != nil && b.Seller.SellerID != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO sellers(seller_id,name,official_name,updated_at) VALUES(?,?,?,?) ON CONFLICT(seller_id) DO UPDATE SET name=excluded.name,official_name=excluded.official_name,updated_at=excluded.updated_at`, b.Seller.SellerID, b.Seller.Name, b.Seller.OfficialName, now)
		if err != nil {
			return false, "", err
		}
	}
	acceptedSet := map[string]bool{}
	for _, v := range b.Variants {
		acceptedSet[v.ItemNumber] = true
	}
	for _, o := range b.Offers {
		if o.ListingID == "" || !acceptedSet[o.ItemNumber] {
			continue
		}
		var seller any
		if o.SellerID != "" {
			seller = o.SellerID
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO offers(listing_id,item_number,seller_id,selling_price,currency,in_stock,scraped_at,updated_at) VALUES(?,?,?,?,?,NULL,?,?) ON CONFLICT(listing_id) DO UPDATE SET item_number=excluded.item_number,seller_id=excluded.seller_id,selling_price=excluded.selling_price,currency=excluded.currency,in_stock=NULL,scraped_at=excluded.scraped_at,updated_at=excluded.updated_at`, o.ListingID, o.ItemNumber, seller, o.SellingPrice, o.Currency, p.ScrapedAt, now)
		if err != nil {
			return false, "", err
		}
	}
	return true, "", nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, jobID int64, from, to, queue, reason string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO job_events(job_id,from_state,to_state,queue_name,reason,created_at) VALUES(?,?,?,?,?,?)`, jobID, from, to, queue, reason, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (d *DB) Metadata(ctx context.Context, key string) (string, error) {
	var v string
	err := d.sql.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=?`, key).Scan(&v)
	return v, err
}
func (d *DB) SetMetadata(ctx context.Context, key, value string) error {
	_, err := d.sql.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (d *DB) EnqueueSeedPages(ctx context.Context, urls []string) (int64, error) {
	var added int64
	now := time.Now().UTC().Format(time.RFC3339)
	for _, rawURL := range urls {
		res, err := d.sql.ExecContext(ctx, `INSERT OR IGNORE INTO seed_pages(url,state,updated_at) VALUES(?,'pending',?)`, rawURL, now)
		if err != nil {
			return added, err
		}
		n, _ := res.RowsAffected()
		added += n
	}
	return added, nil
}

func (d *DB) ClaimSeedPage(ctx context.Context) (string, bool, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	var rawURL string
	err = tx.QueryRowContext(ctx, `SELECT url FROM seed_pages WHERE state IN ('pending','retry') AND available_at<=? ORDER BY updated_at LIMIT 1`, float64(time.Now().UnixNano())/1e9).Scan(&rawURL)
	if err == sql.ErrNoRows {
		_ = tx.Commit()
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE seed_pages SET state='processing',attempts=attempts+1,updated_at=? WHERE url=? AND state IN ('pending','retry')`, time.Now().UTC().Format(time.RFC3339), rawURL)
	if err != nil {
		return "", false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		_ = tx.Commit()
		return "", false, nil
	}
	return rawURL, true, tx.Commit()
}

func (d *DB) FinishSeedPage(ctx context.Context, rawURL string, success bool, reason string) error {
	state := "done"
	available := float64(0)
	if !success {
		state = "retry"
		available = float64(time.Now().Add(time.Minute).UnixNano()) / 1e9
	}
	_, err := d.sql.ExecContext(ctx, `UPDATE seed_pages SET state=?,available_at=?,last_error=?,updated_at=? WHERE url=?`, state, available, reason, time.Now().UTC().Format(time.RFC3339), rawURL)
	return err
}

func (d *DB) DisableBypassQueue(ctx context.Context) (int64, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,state FROM jobs WHERE queue_name='cloudbypass' AND state IN ('pending','retry','processing')`)
	if err != nil {
		return 0, err
	}
	type bypassJob struct {
		id    int64
		state string
	}
	var jobs []bypassJob
	for rows.Next() {
		var job bypassJob
		if err := rows.Scan(&job.id, &job.state); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	now := time.Now()
	for _, job := range jobs {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET queue_name='direct',state='retry',available_at=?,last_error='cloudbypass_disabled',lease_until=NULL,worker_id=NULL,updated_at=? WHERE id=? AND queue_name='cloudbypass' AND state=?`, float64(now.UnixNano())/1e9, now.UTC().Format(time.RFC3339), job.id, job.state)
		if err != nil {
			return 0, err
		}
		if changed, _ := res.RowsAffected(); changed == 1 {
			if err := insertEvent(ctx, tx, job.id, job.state, "retry", "direct", "cloudbypass_disabled"); err != nil {
				return 0, err
			}
		}
	}
	return int64(len(jobs)), tx.Commit()
}

func (d *DB) Snapshot(ctx context.Context) (Counts, error) {
	out := Counts{Queues: map[string]map[string]int64{}, Tables: map[string]int64{}, Minute: map[string]float64{}, HTTP: map[string]int64{}, Quality: map[string]float64{}, Discovery: map[string]string{}}
	for _, queue := range []string{"direct", "cloudbypass"} {
		out.Queues[queue] = map[string]int64{}
		for _, state := range []string{"pending", "processing", "retry", "done", "skip"} {
			out.Queues[queue][state] = 0
		}
	}
	rows, err := d.sql.QueryContext(ctx, `SELECT queue_name,state,COUNT(*) FROM jobs GROUP BY queue_name,state`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var q, s string
		var n int64
		if err := rows.Scan(&q, &s, &n); err != nil {
			rows.Close()
			return out, err
		}
		if out.Queues[q] == nil {
			out.Queues[q] = map[string]int64{}
		}
		out.Queues[q][s] = n
	}
	rows.Close()
	for _, table := range []string{"products", "variants", "sellers", "offers", "reviews", "candidate_sources"} {
		var count int64
		if err := d.sql.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return out, err
		}
		out.Tables[table] = count
	}
	out.Goal.QualifiedProducts = out.Tables["products"]
	out.Goal.VariantRows = out.Tables["variants"]
	minuteAgo := float64(time.Now().Add(-time.Minute).UnixNano()) / 1e9
	metricRows, err := d.sql.QueryContext(ctx, `SELECT kind,COUNT(*),COALESCE(SUM(value),0),COALESCE(SUM(bytes),0) FROM metric_events WHERE created_at>=? GROUP BY kind`, minuteAgo)
	if err != nil {
		return out, err
	}
	for metricRows.Next() {
		var kind string
		var count, value, bytes float64
		if err := metricRows.Scan(&kind, &count, &value, &bytes); err != nil {
			metricRows.Close()
			return out, err
		}
		out.Minute[kind] = count
		out.Minute[kind+"_value"] = value
		out.Minute["response_bytes"] += bytes
		if !strings.HasPrefix(kind, "reason:") {
			out.Minute["response_events"] += count
		}
	}
	metricRows.Close()
	out.Traffic.LastMinuteBytes = int64(math.Round(out.Minute["response_bytes"]))
	out.Traffic.LastMinuteGB = float64(out.Traffic.LastMinuteBytes) / 1_000_000_000
	var totalResponseBytes float64
	_ = d.sql.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key='response_bytes'),0)`).Scan(&totalResponseBytes)
	out.Traffic.TotalResponseBytes = int64(math.Round(totalResponseBytes))
	out.Traffic.TotalResponseGB = float64(out.Traffic.TotalResponseBytes) / 1_000_000_000
	out.Traffic.ProjectionProducts = out.Goal.QualifiedProducts
	if out.Goal.QualifiedProducts > 0 {
		out.Traffic.AverageBytesPerProduct = float64(out.Traffic.TotalResponseBytes) / float64(out.Goal.QualifiedProducts)
	}
	out.Ingest.ProductsPerMinute = int64(math.Round(out.Minute[string(model.ResultDone)]))
	out.Ingest.VariantsPerMinute = int64(math.Round(out.Minute[string(model.ResultDone)+"_value"]))
	out.Ingest.AttemptsPerMinute = int64(math.Round(out.Minute["response_events"]))
	out.Ingest.FailuresPerMinute = int64(math.Round(
		out.Minute[string(model.ResultRetry)] +
			out.Minute[string(model.ResultSkip)] +
			out.Minute[string(model.ResultParserFailure)] +
			out.Minute[string(model.ResultQualityRetry)] +
			out.Minute[string(model.ResultChallenge)],
	))
	out.Ingest.SitemapCandidatesPerMinute = int64(math.Round(out.Minute[MetricDiscoverySitemap+"_value"]))
	out.Ingest.PublicCandidatesPerMinute = int64(math.Round(out.Minute[MetricDiscoveryPublicPage+"_value"]))
	out.Ingest.CandidatesPerMinute = out.Ingest.SitemapCandidatesPerMinute + out.Ingest.PublicCandidatesPerMinute
	d.loadSuccessMetrics(ctx, &out)
	for _, status := range []int{403, 429, 503, 404} {
		var n float64
		_ = d.sql.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key=?),0)`, fmt.Sprintf("http:%d", status)).Scan(&n)
		out.HTTP[fmt.Sprintf("%d", status)] = int64(math.Round(n))
	}
	_ = d.sql.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN cooldown_until>? THEN 1 ELSE 0 END),0),COALESCE(SUM(successes),0),COALESCE(SUM(failures),0) FROM proxy_stats`, float64(time.Now().UnixNano())/1e9).Scan(&out.Proxy.Total, &out.Proxy.Cooling, &out.Proxy.Successes, &out.Proxy.Failures)
	_ = d.sql.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(success),0),COALESCE(SUM(CASE WHEN success=0 THEN 1 ELSE 0 END),0),COALESCE(SUM(cost),0) FROM bypass_usage`).Scan(&out.Bypass.Attempts, &out.Bypass.Successes, &out.Bypass.Failures, &out.Bypass.Cost)
	var total, complete int64
	_ = d.sql.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN variation_status='complete' THEN 1 ELSE 0 END),0) FROM variants`).Scan(&total, &complete)
	if total > 0 {
		out.Quality["variation_complete_pct"] = float64(complete) * 100 / float64(total)
		var priced int64
		_ = d.sql.QueryRowContext(ctx, `SELECT COALESCE(SUM(selling_price<>''),0) FROM variants`).Scan(&priced)
		out.Quality["price_pct"] = float64(priced) * 100 / float64(total)
	}
	var products, title, brand, image, category int64
	_ = d.sql.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(title<>''),0),COALESCE(SUM(brand<>''),0),COALESCE(SUM(main_image<>''),0),COALESCE(SUM(category_path<>''),0) FROM products`).Scan(&products, &title, &brand, &image, &category)
	if products > 0 {
		out.Quality["title_pct"] = float64(title) * 100 / float64(products)
		out.Quality["brand_pct"] = float64(brand) * 100 / float64(products)
		out.Quality["main_image_pct"] = float64(image) * 100 / float64(products)
		out.Quality["category_pct"] = float64(category) * 100 / float64(products)
	}
	metaRows, _ := d.sql.QueryContext(ctx, `SELECT key,value FROM metadata WHERE key LIKE 'discovery_%'`)
	if metaRows != nil {
		for metaRows.Next() {
			var k, v string
			_ = metaRows.Scan(&k, &v)
			out.Discovery[k] = v
		}
		metaRows.Close()
	}
	var highPriority, neutralPriority, lowPriority int64
	_ = d.sql.QueryRowContext(ctx, `SELECT
COALESCE(SUM(CASE WHEN priority>0 THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN priority=0 THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN priority<0 THEN 1 ELSE 0 END),0)
FROM jobs WHERE state IN ('pending','retry')`).Scan(&highPriority, &neutralPriority, &lowPriority)
	out.Discovery["priority_high"] = strconv.FormatInt(highPriority, 10)
	out.Discovery["priority_neutral"] = strconv.FormatInt(neutralPriority, 10)
	out.Discovery["priority_low"] = strconv.FormatInt(lowPriority, 10)
	d.loadBackfillSummary(ctx, &out)
	return out, nil
}

// MonitorSnapshot avoids full scans of product, variant and quality tables.
// It is safe to refresh frequently while a million-product writer is active;
// the full Snapshot remains available for deliberate status/quality checks.
func (d *DB) MonitorSnapshot(ctx context.Context) (Counts, error) {
	out := Counts{Queues: map[string]map[string]int64{}, Tables: map[string]int64{}, Minute: map[string]float64{}, HTTP: map[string]int64{}, Quality: map[string]float64{}, Discovery: map[string]string{}}
	for _, queue := range []string{"direct", "cloudbypass"} {
		out.Queues[queue] = map[string]int64{}
		for _, state := range []string{"pending", "processing", "retry", "done", "skip"} {
			out.Queues[queue][state] = 0
		}
	}
	rows, err := d.sql.QueryContext(ctx, `SELECT queue_name,state,COUNT(*) FROM jobs GROUP BY queue_name,state`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var queue, state string
		var count int64
		if err := rows.Scan(&queue, &state, &count); err != nil {
			rows.Close()
			return out, err
		}
		if out.Queues[queue] == nil {
			out.Queues[queue] = map[string]int64{}
		}
		out.Queues[queue][state] = count
	}
	if err := rows.Close(); err != nil {
		return out, err
	}
	if err := d.sql.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM metadata WHERE key='accepted_product_count'`).Scan(&out.Goal.QualifiedProducts); err != nil {
		return out, err
	}
	if err := d.sql.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM metadata WHERE key='accepted_variant_count'`).Scan(&out.Goal.VariantRows); err != nil {
		return out, err
	}

	minuteAgo := float64(time.Now().Add(-time.Minute).UnixNano()) / 1e9
	metricRows, err := d.sql.QueryContext(ctx, `SELECT kind,COUNT(*),COALESCE(SUM(value),0),COALESCE(SUM(bytes),0) FROM metric_events WHERE created_at>=? GROUP BY kind`, minuteAgo)
	if err != nil {
		return out, err
	}
	for metricRows.Next() {
		var kind string
		var count, value, responseBytes float64
		if err := metricRows.Scan(&kind, &count, &value, &responseBytes); err != nil {
			metricRows.Close()
			return out, err
		}
		out.Minute[kind] = count
		out.Minute[kind+"_value"] = value
		out.Minute["response_bytes"] += responseBytes
		if !strings.HasPrefix(kind, "reason:") {
			out.Minute["response_events"] += count
		}
	}
	if err := metricRows.Close(); err != nil {
		return out, err
	}
	out.Traffic.LastMinuteBytes = int64(math.Round(out.Minute["response_bytes"]))
	out.Traffic.LastMinuteGB = float64(out.Traffic.LastMinuteBytes) / 1_000_000_000
	var totalResponseBytes float64
	_ = d.sql.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key='response_bytes'),0)`).Scan(&totalResponseBytes)
	out.Traffic.TotalResponseBytes = int64(math.Round(totalResponseBytes))
	out.Traffic.TotalResponseGB = float64(out.Traffic.TotalResponseBytes) / 1_000_000_000
	out.Traffic.ProjectionProducts = out.Goal.QualifiedProducts
	if out.Goal.QualifiedProducts > 0 {
		out.Traffic.AverageBytesPerProduct = float64(out.Traffic.TotalResponseBytes) / float64(out.Goal.QualifiedProducts)
	}
	out.Ingest.ProductsPerMinute = int64(math.Round(out.Minute[string(model.ResultDone)]))
	out.Ingest.VariantsPerMinute = int64(math.Round(out.Minute[string(model.ResultDone)+"_value"]))
	out.Ingest.AttemptsPerMinute = int64(math.Round(out.Minute["response_events"]))
	out.Ingest.FailuresPerMinute = int64(math.Round(out.Minute[string(model.ResultRetry)] + out.Minute[string(model.ResultSkip)] + out.Minute[string(model.ResultParserFailure)] + out.Minute[string(model.ResultQualityRetry)] + out.Minute[string(model.ResultChallenge)]))
	out.Ingest.SitemapCandidatesPerMinute = int64(math.Round(out.Minute[MetricDiscoverySitemap+"_value"]))
	out.Ingest.PublicCandidatesPerMinute = int64(math.Round(out.Minute[MetricDiscoveryPublicPage+"_value"]))
	out.Ingest.CandidatesPerMinute = out.Ingest.SitemapCandidatesPerMinute + out.Ingest.PublicCandidatesPerMinute
	d.loadSuccessMetrics(ctx, &out)

	for _, status := range []int{403, 429, 503, 404} {
		var count float64
		_ = d.sql.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key=?),0)`, fmt.Sprintf("http:%d", status)).Scan(&count)
		out.HTTP[strconv.Itoa(status)] = int64(math.Round(count))
	}
	_ = d.sql.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN cooldown_until>? THEN 1 ELSE 0 END),0),COALESCE(SUM(successes),0),COALESCE(SUM(failures),0) FROM proxy_stats`, float64(time.Now().UnixNano())/1e9).Scan(&out.Proxy.Total, &out.Proxy.Cooling, &out.Proxy.Successes, &out.Proxy.Failures)
	metaRows, _ := d.sql.QueryContext(ctx, `SELECT key,value FROM metadata WHERE key LIKE 'discovery_%'`)
	if metaRows != nil {
		for metaRows.Next() {
			var key, value string
			_ = metaRows.Scan(&key, &value)
			out.Discovery[key] = value
		}
		metaRows.Close()
	}
	var highPriority, neutralPriority, lowPriority int64
	_ = d.sql.QueryRowContext(ctx, `SELECT
COALESCE(SUM(CASE WHEN priority>0 THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN priority=0 THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN priority<0 THEN 1 ELSE 0 END),0)
FROM jobs WHERE state IN ('pending','retry')`).Scan(&highPriority, &neutralPriority, &lowPriority)
	out.Discovery["priority_high"] = strconv.FormatInt(highPriority, 10)
	out.Discovery["priority_neutral"] = strconv.FormatInt(neutralPriority, 10)
	out.Discovery["priority_low"] = strconv.FormatInt(lowPriority, 10)
	d.loadBackfillSummary(ctx, &out)
	return out, nil
}

func recordResponseMetric(ctx context.Context, tx *sql.Tx, kind string, value float64, httpStatus int, queue string, responseBytes int64, at time.Time) error {
	if responseBytes < 0 {
		responseBytes = 0
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO metric_events(kind,value,http_status,queue_name,bytes,created_at) VALUES(?,?,?,?,?,?)`, kind, value, httpStatus, queue, responseBytes, float64(at.UnixNano())/1e9); err != nil {
		return err
	}
	for key, increment := range map[string]float64{
		"response_bytes":  float64(responseBytes),
		"response_events": 1,
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO metric_totals(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=metric_totals.value+excluded.value`, key, increment); err != nil {
			return err
		}
	}
	if isScrapeOutcome(kind) {
		key := "outcome:" + kind
		if _, err := tx.ExecContext(ctx, `INSERT INTO metric_totals(key,value) VALUES(?,1) ON CONFLICT(key) DO UPDATE SET value=metric_totals.value+1`, key); err != nil {
			return err
		}
	}
	if httpStatus > 0 {
		key := fmt.Sprintf("http:%d", httpStatus)
		if _, err := tx.ExecContext(ctx, `INSERT INTO metric_totals(key,value) VALUES(?,1) ON CONFLICT(key) DO UPDATE SET value=metric_totals.value+1`, key); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) loadSuccessMetrics(ctx context.Context, out *Counts) {
	for _, kind := range scrapeOutcomeKinds() {
		out.Ingest.CumulativeAttempts += metricTotalInt(ctx, d.sql, "outcome:"+kind)
	}
	out.Ingest.CumulativeSuccesses = metricTotalInt(ctx, d.sql, "outcome:"+string(model.ResultDone))
	out.Ingest.SuccessesPerMinute = int64(math.Round(out.Minute[string(model.ResultDone)]))
	minuteAttempts := float64(0)
	for _, kind := range scrapeOutcomeKinds() {
		minuteAttempts += out.Minute[kind]
	}
	if minuteAttempts > 0 {
		out.Ingest.SuccessRatePct = float64(out.Ingest.SuccessesPerMinute) * 100 / minuteAttempts
	}
	if out.Ingest.CumulativeAttempts > 0 {
		out.Ingest.CumulativeSuccessRatePct = float64(out.Ingest.CumulativeSuccesses) * 100 / float64(out.Ingest.CumulativeAttempts)
	}
}

func scrapeOutcomeKinds() []string {
	return []string{
		string(model.ResultDone), string(model.ResultRetry), string(model.ResultSkip),
		string(model.ResultParserFailure), string(model.ResultQualityRetry), string(model.ResultChallenge),
	}
}

func isScrapeOutcome(kind string) bool {
	for _, candidate := range scrapeOutcomeKinds() {
		if kind == candidate {
			return true
		}
	}
	return false
}

func metricTotalInt(ctx context.Context, db *sql.DB, key string) int64 {
	var value float64
	_ = db.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM metric_totals WHERE key=?),0)`, key).Scan(&value)
	return int64(math.Round(value))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
