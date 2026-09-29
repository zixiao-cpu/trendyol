package store

import (
	"context"
	"database/sql"
	"fmt"
)

const schema = `
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 product_id TEXT NOT NULL UNIQUE,
 path TEXT NOT NULL,
 url TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','processing','retry','done','skip')),
 queue_name TEXT NOT NULL DEFAULT 'direct' CHECK(queue_name IN ('direct','cloudbypass')),
 attempts INTEGER NOT NULL DEFAULT 0,
 quality_attempts INTEGER NOT NULL DEFAULT 0,
	 bypass_attempts INTEGER NOT NULL DEFAULT 0,
	 priority INTEGER NOT NULL DEFAULT 0,
 available_at REAL NOT NULL DEFAULT 0,
 lease_until REAL,
 worker_id TEXT,
 last_error TEXT,
 http_status INTEGER,
 discovered_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(queue_name,state,priority DESC,available_at,id);
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(state,lease_until);
CREATE INDEX IF NOT EXISTS jobs_content_backfill_idx ON jobs(priority,state,available_at) WHERE priority>=400;
CREATE TABLE IF NOT EXISTS candidate_sources (
 product_id TEXT NOT NULL REFERENCES jobs(product_id) ON DELETE CASCADE,
 source TEXT NOT NULL,source_ref TEXT NOT NULL DEFAULT '',sightings INTEGER NOT NULL DEFAULT 1,
 first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL,
 PRIMARY KEY(product_id,source)
);
CREATE INDEX IF NOT EXISTS candidate_sources_source_idx ON candidate_sources(source,last_seen_at);
CREATE TABLE IF NOT EXISTS job_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
 from_state TEXT,
 to_state TEXT NOT NULL,
 queue_name TEXT NOT NULL,
 reason TEXT,
 created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS job_events_job_reason_idx ON job_events(job_id,reason);
CREATE TABLE IF NOT EXISTS products (
 product_id TEXT PRIMARY KEY,url TEXT NOT NULL,title TEXT NOT NULL,brand TEXT NOT NULL,
 main_image TEXT NOT NULL,images TEXT NOT NULL,description TEXT NOT NULL DEFAULT '',
 features TEXT NOT NULL,specs TEXT NOT NULL,reviews TEXT NOT NULL DEFAULT '[]',
 category_path TEXT NOT NULL,category_level_1 TEXT NOT NULL,category_level_2 TEXT NOT NULL,
 category_level_3 TEXT NOT NULL,category_leaf TEXT NOT NULL,category_node_ids TEXT NOT NULL,
 category_source TEXT NOT NULL,category_complete TEXT NOT NULL,scraped_at TEXT NOT NULL,updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS variants (
 item_number TEXT PRIMARY KEY,product_id TEXT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
 product_group_id TEXT NOT NULL,color TEXT NOT NULL DEFAULT '',capacity TEXT NOT NULL DEFAULT '',
 material TEXT NOT NULL DEFAULT '',selling_price TEXT NOT NULL,currency TEXT NOT NULL,
 variation_theme TEXT NOT NULL,variation_value TEXT NOT NULL,variation_status TEXT NOT NULL DEFAULT 'complete',
 category_key TEXT NOT NULL,scraped_at TEXT NOT NULL,updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS variants_product_idx ON variants(product_id);
CREATE INDEX IF NOT EXISTS variants_group_idx ON variants(product_group_id);
CREATE INDEX IF NOT EXISTS variants_category_idx ON variants(category_key);
CREATE INDEX IF NOT EXISTS variants_category_product_idx ON variants(category_key,product_id);
CREATE TABLE IF NOT EXISTS category_product_counts (
 category_key TEXT PRIMARY KEY,product_count INTEGER NOT NULL DEFAULT 0
);
CREATE TRIGGER IF NOT EXISTS products_counter_insert AFTER INSERT ON products BEGIN
 UPDATE metadata SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='accepted_product_count';
END;
CREATE TRIGGER IF NOT EXISTS products_counter_delete AFTER DELETE ON products BEGIN
 UPDATE metadata SET value=CAST(MAX(CAST(value AS INTEGER)-1,0) AS TEXT) WHERE key='accepted_product_count';
END;
CREATE TRIGGER IF NOT EXISTS variants_counter_insert AFTER INSERT ON variants BEGIN
 UPDATE metadata SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='accepted_variant_count';
END;
CREATE TRIGGER IF NOT EXISTS variants_counter_delete AFTER DELETE ON variants BEGIN
 UPDATE metadata SET value=CAST(MAX(CAST(value AS INTEGER)-1,0) AS TEXT) WHERE key='accepted_variant_count';
END;
CREATE TRIGGER IF NOT EXISTS category_products_counter_insert AFTER INSERT ON variants
WHEN (SELECT COUNT(*) FROM variants WHERE product_id=NEW.product_id AND category_key=NEW.category_key)=1 BEGIN
 INSERT INTO category_product_counts(category_key,product_count) VALUES(NEW.category_key,1)
 ON CONFLICT(category_key) DO UPDATE SET product_count=category_product_counts.product_count+1;
END;
CREATE TRIGGER IF NOT EXISTS category_products_counter_delete AFTER DELETE ON variants
WHEN NOT EXISTS(SELECT 1 FROM variants WHERE product_id=OLD.product_id AND category_key=OLD.category_key) BEGIN
 UPDATE category_product_counts SET product_count=MAX(product_count-1,0) WHERE category_key=OLD.category_key;
END;
CREATE TABLE IF NOT EXISTS sellers (
 seller_id TEXT PRIMARY KEY,name TEXT NOT NULL DEFAULT '',official_name TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS offers (
 listing_id TEXT PRIMARY KEY,item_number TEXT NOT NULL REFERENCES variants(item_number) ON DELETE CASCADE,
 seller_id TEXT REFERENCES sellers(seller_id),selling_price TEXT NOT NULL,currency TEXT NOT NULL,
 in_stock INTEGER,scraped_at TEXT NOT NULL,updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS offers_item_idx ON offers(item_number);
CREATE TABLE IF NOT EXISTS reviews (
 review_key TEXT PRIMARY KEY,product_id TEXT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
 stars REAL NOT NULL,title TEXT NOT NULL DEFAULT '',body TEXT NOT NULL DEFAULT '',review_date TEXT NOT NULL DEFAULT '',
 author TEXT NOT NULL DEFAULT '',images TEXT NOT NULL DEFAULT '[]',position INTEGER NOT NULL DEFAULT 0,scraped_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reviews_product_idx ON reviews(product_id);
CREATE TABLE IF NOT EXISTS metric_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,value REAL NOT NULL DEFAULT 1,
 http_status INTEGER,queue_name TEXT,bytes INTEGER NOT NULL DEFAULT 0,created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS metric_events_time_idx ON metric_events(created_at,kind);
CREATE TABLE IF NOT EXISTS metric_totals (
 key TEXT PRIMARY KEY,value REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS proxy_stats (
 proxy_key TEXT PRIMARY KEY,successes INTEGER NOT NULL DEFAULT 0,failures INTEGER NOT NULL DEFAULT 0,
 cooldown_until REAL NOT NULL DEFAULT 0,updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bypass_usage (
 id INTEGER PRIMARY KEY AUTOINCREMENT,job_id INTEGER,success INTEGER NOT NULL,duration_ms INTEGER NOT NULL,
 cost REAL NOT NULL DEFAULT 0,error TEXT,created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS response_failures (
 id INTEGER PRIMARY KEY AUTOINCREMENT,job_id INTEGER NOT NULL,body_sha256 TEXT NOT NULL,
 fixture_path TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seed_pages (
 url TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
 available_at REAL NOT NULL DEFAULT 0,last_error TEXT,updated_at TEXT NOT NULL
);
`

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return err
	}
	for _, migration := range []struct{ table, column, ddl string }{
		{"jobs", "quality_attempts", "ALTER TABLE jobs ADD COLUMN quality_attempts INTEGER NOT NULL DEFAULT 0"},
		{"jobs", "bypass_attempts", "ALTER TABLE jobs ADD COLUMN bypass_attempts INTEGER NOT NULL DEFAULT 0"},
		{"jobs", "priority", "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"},
		{"variants", "variation_status", "ALTER TABLE variants ADD COLUMN variation_status TEXT NOT NULL DEFAULT 'complete'"},
		{"reviews", "position", "ALTER TABLE reviews ADD COLUMN position INTEGER NOT NULL DEFAULT 0"},
	} {
		has, err := hasColumn(ctx, db, migration.table, migration.column)
		if err != nil {
			return err
		}
		if !has {
			if _, err := db.ExecContext(ctx, migration.ddl); err != nil {
				return fmt.Errorf("migrate %s.%s: %w", migration.table, migration.column, err)
			}
		}
	}
	// Recreate the claim index because SQLite's IF NOT EXISTS does not update
	// the definition created by an older schema.
	if _, err := db.ExecContext(ctx, `DROP INDEX IF EXISTS jobs_claim_idx;
CREATE INDEX jobs_claim_idx ON jobs(queue_name,state,priority DESC,available_at,id);`); err != nil {
		return fmt.Errorf("migrate jobs priority index: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO metadata(key,value)
SELECT 'accepted_product_count',CAST(COUNT(*) AS TEXT) FROM products`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO metadata(key,value)
SELECT 'accepted_variant_count',CAST(COUNT(*) AS TEXT) FROM variants`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO category_product_counts(category_key,product_count)
SELECT category_key,COUNT(DISTINCT product_id) FROM variants GROUP BY category_key`); err != nil {
		return err
	}
	// Backfill constant-time monitoring counters once for databases created by
	// older builds. New events update these counters transactionally.
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'response_bytes',COALESCE(SUM(bytes),0) FROM metric_events;
INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'response_events',COUNT(*) FROM metric_events WHERE kind NOT LIKE 'reason:%';
INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'http:403',COUNT(*) FROM metric_events WHERE http_status=403 AND kind NOT LIKE 'reason:%';
INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'http:429',COUNT(*) FROM metric_events WHERE http_status=429 AND kind NOT LIKE 'reason:%';
INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'http:503',COUNT(*) FROM metric_events WHERE http_status=503 AND kind NOT LIKE 'reason:%';
INSERT OR IGNORE INTO metric_totals(key,value)
SELECT 'http:404',COUNT(*) FROM metric_events WHERE http_status=404 AND kind NOT LIKE 'reason:%'`); err != nil {
		return err
	}
	for _, kind := range []string{"done", "retry", "skip", "parser_failure", "quality_retry", "challenge"} {
		if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO metric_totals(key,value)
SELECT ?,COUNT(*) FROM metric_events WHERE kind=?`, "outcome:"+kind, kind); err != nil {
			return err
		}
	}
	_, err := db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('go_schema_version','7')
ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
	return err
}

func hasColumn(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
