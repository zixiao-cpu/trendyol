package store

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"trendyol/internal/model"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "state", "trendyol.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWALBatchClaimRetryRecoveryAndDedupe(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	var mode string
	if err := db.sql.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal=%q err=%v", mode, err)
	}
	c := Candidate{ProductID: "881465224", Path: "/sample-p-881465224", URL: "https://www.trendyol.com/sample-p-881465224"}
	if added, err := db.Enqueue(ctx, []Candidate{c, c}); err != nil || added != 1 {
		t.Fatalf("added=%d err=%v", added, err)
	}
	jobs, err := db.BatchClaim(ctx, "direct", "dispatcher", 10, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs), err)
	}
	result := model.ScrapeResult{Job: jobs[0], Kind: model.ResultRetry, Reason: "http_429", HTTPStatus: 429}
	if err := db.ApplyResult(ctx, result, 100, 10, 5, 100*time.Millisecond, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	var state string
	var available float64
	if err := db.sql.QueryRow(`SELECT state,available_at FROM jobs WHERE id=?`, jobs[0].ID).Scan(&state, &available); err != nil || state != "retry" || available <= float64(time.Now().UnixNano())/1e9 {
		t.Fatalf("state=%s available=%f err=%v", state, available, err)
	}
	if _, err := db.sql.Exec(`UPDATE jobs SET state='processing',lease_until=0`); err != nil {
		t.Fatal(err)
	}
	if recovered, err := db.RecoverStale(ctx); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
}

func TestBatchClaimUsesDiscoveryPriority(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidates := []Candidate{
		{ProductID: "low", Path: "/low-p-1001", URL: "https://www.trendyol.com/low-p-1001", Priority: -40},
		{ProductID: "high", Path: "/high-p-1002", URL: "https://www.trendyol.com/high-p-1002", Priority: 100},
	}
	if _, err := db.Enqueue(ctx, candidates); err != nil {
		t.Fatal(err)
	}
	jobs, err := db.BatchClaim(ctx, "direct", "priority-test", 1, time.Minute)
	if err != nil || len(jobs) != 1 || jobs[0].ProductID != "high" {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
}

func TestContentBackfillClaimExcludesNewCandidates(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	_, err := db.Enqueue(ctx, []Candidate{{ProductID: "old", Path: "/old-p-1001", URL: "https://www.trendyol.com/old-p-1001", Priority: ContentBackfillPriority}, {ProductID: "new", Path: "/new-p-1002", URL: "https://www.trendyol.com/new-p-1002", Priority: 300}, {ProductID: "unrelated-admin", Path: "/admin-p-1003", URL: "https://www.trendyol.com/admin-p-1003", Priority: ContentBackfillPriority}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().Exec(`INSERT INTO products(product_id,url,title,brand,main_image,images,description,features,specs,reviews,category_path,category_level_1,category_level_2,category_level_3,category_leaf,category_node_ids,category_source,category_complete,scraped_at,updated_at)
VALUES('old','https://www.trendyol.com/old-p-1001','old','brand','image','image','','','{}','[]','category','category','','','category','','trendyol_category_tree','true','now','now'),
('unrelated-admin','https://www.trendyol.com/admin-p-1003','admin','brand','image','image','already complete','','{}','[]','category','category','','','category','','trendyol_category_tree','true','now','now')`); err != nil {
		t.Fatal(err)
	}
	jobs, err := db.BatchClaimContentBackfill(ctx, "direct", "backfill-test", 10, time.Minute)
	if err != nil || len(jobs) != 1 || jobs[0].ProductID != "old" {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	active, err := db.ActiveContentBackfill(ctx)
	if err != nil || active != 1 {
		t.Fatalf("active=%d err=%v", active, err)
	}
}

func TestResetCandidatesPreservesAcceptedProducts(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidate := Candidate{ProductID: "kept-product", Path: "/kept-p-3001", URL: "https://www.trendyol.com/kept-p-3001"}
	if _, err := db.Enqueue(ctx, []Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	jobs, err := db.BatchClaim(ctx, "direct", "reset-test", 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs), err)
	}
	if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[0], Kind: model.ResultDone, Bundle: testBundle("kept-product", "category", "sku-kept")}, 10, 0, 5, time.Second, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	removed, err := db.ResetCandidates(ctx)
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	var jobsLeft, productsLeft int
	_ = db.SQL().QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&jobsLeft)
	_ = db.SQL().QueryRow(`SELECT COUNT(*) FROM products`).Scan(&productsLeft)
	if jobsLeft != 0 || productsLeft != 1 {
		t.Fatalf("jobs=%d products=%d", jobsLeft, productsLeft)
	}
}

func TestNormalizeStandaloneParents(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.SQL().Exec(`INSERT INTO products(product_id,url,title,brand,main_image,images,description,features,specs,reviews,category_path,category_level_1,category_level_2,category_level_3,category_leaf,category_node_ids,category_source,category_complete,scraped_at,updated_at) VALUES('p1','u','t','b','i','i','','','{}','[]','c','c','','','c','','trendyol_category_tree','true','2026-08-22T10:00:00Z','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().Exec(`INSERT INTO variants(item_number,product_id,product_group_id,color,capacity,material,selling_price,currency,variation_theme,variation_value,variation_status,category_key,scraped_at,updated_at) VALUES('sku1','p1','group1','','','','60','USD','','','complete','c','2026-08-22T10:00:00Z','now')`); err != nil {
		t.Fatal(err)
	}
	updated, err := db.NormalizeStandaloneParents(ctx)
	if err != nil || updated != 1 {
		t.Fatalf("updated=%d err=%v", updated, err)
	}
	var parent string
	if err := db.SQL().QueryRow(`SELECT product_group_id FROM variants WHERE item_number='sku1'`).Scan(&parent); err != nil || parent != "p1" {
		t.Fatalf("parent=%q err=%v", parent, err)
	}
}

func TestBusinessSaveAndDoneAreOneWriterTransaction(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidate := Candidate{ProductID: "1", Path: "/p-p-1000", URL: "https://www.trendyol.com/p-p-1000"}
	_, _ = db.Enqueue(ctx, []Candidate{candidate})
	jobs, err := db.BatchClaim(ctx, "direct", "dispatcher", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &model.ProductBundle{Product: model.Product{ProductID: "1", URL: candidate.URL, Title: "Ürün", Brand: "Marka", MainImage: "https://cdn/image.jpg", Images: "https://cdn/image.jpg", Description: "", Features: "", Specs: "{}", CategoryPath: "Elektronik", CategoryLevel1: "Elektronik", CategoryLeaf: "Elektronik", CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: "2026-08-22T10:00:00Z"}, Variants: []model.Variant{{ItemNumber: "sku-1", ProductID: "1", ProductGroupID: "1", SellingPrice: "60.00", Currency: "USD", VariationStatus: "complete", CategoryKey: "1", ScrapedAt: "2026-08-22T10:00:00Z"}}, Reviews: []model.Review{{Stars: 5, Body: "İyi", Images: []string{}}}}
	if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[0], Kind: model.ResultDone, Bundle: bundle}, 100, 10, 5, time.Second, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.sql.QueryRow(`SELECT state FROM jobs WHERE id=?`, jobs[0].ID).Scan(&state); err != nil || state != "done" {
		t.Fatalf("state=%s err=%v", state, err)
	}
	counts, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Tables["products"] != 1 || counts.Tables["variants"] != 1 || counts.Tables["reviews"] != 1 {
		t.Fatalf("counts=%+v", counts.Tables)
	}
	if counts.Ingest.CumulativeSuccesses != 1 || counts.Ingest.CumulativeAttempts != 1 || counts.Ingest.CumulativeSuccessRatePct != 100 {
		t.Fatalf("success metrics=%+v", counts.Ingest)
	}
	requeued, err := db.PrepareContentBackfill(ctx)
	if err != nil || requeued != 1 {
		t.Fatalf("content backfill requeued=%d err=%v", requeued, err)
	}
	var priority int
	if err := db.sql.QueryRow(`SELECT state,priority FROM jobs WHERE id=?`, jobs[0].ID).Scan(&state, &priority); err != nil || state != "pending" || priority < 400 {
		t.Fatalf("backfill state=%q priority=%d err=%v", state, priority, err)
	}
	var events int
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM job_events WHERE job_id=? AND reason='content_backfill'`, jobs[0].ID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("backfill events=%d err=%v", events, err)
	}
	if again, err := db.PrepareContentBackfill(ctx); err != nil || again != 0 {
		t.Fatalf("idempotent backfill requeued=%d err=%v", again, err)
	}
}

func TestProductTargetCountsProductsAndKeepsAllVariants(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidates := []Candidate{
		{ProductID: "product-1", Path: "/one-p-1001", URL: "https://www.trendyol.com/one-p-1001"},
		{ProductID: "product-2", Path: "/two-p-1002", URL: "https://www.trendyol.com/two-p-1002"},
	}
	if _, err := db.Enqueue(ctx, candidates); err != nil {
		t.Fatal(err)
	}
	jobs, err := db.BatchClaim(ctx, "direct", "target-test", 2, time.Minute)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs=%d err=%v", len(jobs), err)
	}
	first := testBundle("product-1", "category-a", "sku-1", "sku-2", "sku-3")
	if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[0], Kind: model.ResultDone, Bundle: first}, 1, 0, 5, time.Second, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	second := testBundle("product-2", "category-b", "sku-4")
	if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[1], Kind: model.ResultDone, Bundle: second}, 1, 0, 5, time.Second, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	products, err := db.ProductCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	variants, err := db.VariantCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if products != 1 || variants != 3 {
		t.Fatalf("products=%d variants=%d", products, variants)
	}
	var state, reason string
	if err := db.sql.QueryRow(`SELECT state,last_error FROM jobs WHERE product_id='product-2'`).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "skip" || reason != "product_target_reached" {
		t.Fatalf("state=%q reason=%q", state, reason)
	}
}

func TestCategoryCapCountsWholeProductsNotVariants(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidates := []Candidate{
		{ProductID: "product-a", Path: "/a-p-2001", URL: "https://www.trendyol.com/a-p-2001"},
		{ProductID: "product-b", Path: "/b-p-2002", URL: "https://www.trendyol.com/b-p-2002"},
		{ProductID: "product-c", Path: "/c-p-2003", URL: "https://www.trendyol.com/c-p-2003"},
	}
	if _, err := db.Enqueue(ctx, candidates); err != nil {
		t.Fatal(err)
	}
	jobs, err := db.BatchClaim(ctx, "direct", "category-test", 3, time.Minute)
	if err != nil || len(jobs) != 3 {
		t.Fatalf("jobs=%d err=%v", len(jobs), err)
	}
	bundles := []*model.ProductBundle{
		testBundle("product-a", "category-a", "a-1", "a-2"),
		testBundle("product-b", "category-a", "b-1"),
		testBundle("product-c", "category-b", "c-1", "c-2", "c-3"),
	}
	for i := range jobs {
		if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[i], Kind: model.ResultDone, Bundle: bundles[i]}, 10, 1, 5, time.Second, t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
	products, _ := db.ProductCount(ctx)
	variants, _ := db.VariantCount(ctx)
	if products != 2 || variants != 5 {
		t.Fatalf("products=%d variants=%d", products, variants)
	}
	var state, reason string
	if err := db.sql.QueryRow(`SELECT state,last_error FROM jobs WHERE product_id='product-b'`).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "skip" || reason != "category_product_cap_reached" {
		t.Fatalf("state=%q reason=%q", state, reason)
	}
}

func testBundle(productID, categoryKey string, itemNumbers ...string) *model.ProductBundle {
	variants := make([]model.Variant, 0, len(itemNumbers))
	for _, itemNumber := range itemNumbers {
		variants = append(variants, model.Variant{ItemNumber: itemNumber, ProductID: productID, ProductGroupID: productID, SellingPrice: "60.00", Currency: "USD", VariationStatus: "complete", CategoryKey: categoryKey, ScrapedAt: "2026-08-22T10:00:00Z"})
	}
	return &model.ProductBundle{
		Product:  model.Product{ProductID: productID, URL: "https://www.trendyol.com/-p-" + productID, Title: "Ürün", Brand: "Marka", MainImage: "https://cdn/image.jpg", Images: "https://cdn/image.jpg", Specs: "{}", CategoryPath: categoryKey, CategoryLevel1: categoryKey, CategoryLeaf: categoryKey, CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: "2026-08-22T10:00:00Z"},
		Variants: variants,
	}
}

func TestMigrationKeepsPythonSchemaRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "python-compatible.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Enqueue(ctx, []Candidate{{ProductID: "881465224", Path: "/sample-p-881465224", URL: "https://www.trendyol.com/sample-p-881465224"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetMetadata(ctx, "python_marker", "kept"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, err := reopened.Metadata(ctx, "python_marker"); err != nil || value != "kept" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	var jobs int
	if err := reopened.sql.QueryRow(`SELECT COUNT(*) FROM jobs WHERE product_id='881465224'`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("jobs=%d err=%v", jobs, err)
	}
}

func TestAdministrativeRecoveryWritesJobEvents(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	candidates := []Candidate{
		{ProductID: "recover-direct", Path: "/recover-p-1", URL: "https://www.trendyol.com/recover-p-1"},
		{ProductID: "disable-bypass", Path: "/bypass-p-2", URL: "https://www.trendyol.com/bypass-p-2"},
	}
	if _, err := db.Enqueue(ctx, candidates); err != nil {
		t.Fatal(err)
	}
	claimed, err := db.BatchClaim(ctx, "direct", "owner-1", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%d err=%v", len(claimed), err)
	}
	if recovered, err := db.RecoverWorker(ctx, "owner-1"); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	if _, err := db.sql.Exec(`UPDATE jobs SET queue_name='cloudbypass',state='pending' WHERE product_id='disable-bypass'`); err != nil {
		t.Fatal(err)
	}
	if moved, err := db.DisableBypassQueue(ctx); err != nil || moved != 1 {
		t.Fatalf("moved=%d err=%v", moved, err)
	}
	var recoverEvents, disabledEvents int
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM job_events WHERE reason='worker_recovered'`).Scan(&recoverEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM job_events WHERE reason='cloudbypass_disabled' AND queue_name='direct'`).Scan(&disabledEvents); err != nil {
		t.Fatal(err)
	}
	if recoverEvents != 1 || disabledEvents != 1 {
		t.Fatalf("recovery events=%d disabled events=%d", recoverEvents, disabledEvents)
	}
}

func TestSnapshotIncludesDiscoveryRefillAndProxyTraffic(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	if err := db.RecordDiscovery(ctx, MetricDiscoverySitemap, 125, 200, 2_000_000_000); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordDiscovery(ctx, MetricDiscoveryPublicPage, 7, 200, 500_000_000); err != nil {
		t.Fatal(err)
	}
	status, err := db.MonitorSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Ingest.CandidatesPerMinute != 132 || status.Ingest.SitemapCandidatesPerMinute != 125 || status.Ingest.PublicCandidatesPerMinute != 7 {
		t.Fatalf("ingest=%+v", status.Ingest)
	}
	if status.Traffic.LastMinuteBytes != 2_500_000_000 || status.Traffic.TotalResponseBytes != 2_500_000_000 || math.Abs(status.Traffic.LastMinuteGB-2.5) > 0.000001 {
		t.Fatalf("traffic=%+v", status.Traffic)
	}
	if status.Ingest.AttemptsPerMinute != 2 {
		t.Fatalf("attempts=%d", status.Ingest.AttemptsPerMinute)
	}
}
