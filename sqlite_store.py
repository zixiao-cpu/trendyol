# -*- coding: utf-8 -*-
"""SQLite WAL persistent queue and normalized e-commerce storage."""
import hashlib
import json
import os
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone


JOB_STATES = ("pending", "processing", "retry", "done", "skip")
QUEUE_NAMES = ("direct", "cloudbypass")


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def immediate(connection):
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
        connection.commit()
    except Exception:
        connection.rollback()
        raise


class SQLiteStore:
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.local = threading.local()

    def connection(self):
        connection = getattr(self.local, "connection", None)
        if connection is None:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=30)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            self.local.connection = connection
        return connection

    def close_thread(self):
        connection = getattr(self.local, "connection", None)
        if connection is not None:
            connection.close()
            self.local.connection = None

    def initialize(self):
        connection = self.connection()
        connection.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL,
            url TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK(state IN ('pending','processing','retry','done','skip')),
            queue_name TEXT NOT NULL DEFAULT 'direct'
                CHECK(queue_name IN ('direct','cloudbypass')),
            attempts INTEGER NOT NULL DEFAULT 0,
            available_at REAL NOT NULL DEFAULT 0,
            lease_until REAL,
            worker_id TEXT,
            last_error TEXT,
            http_status INTEGER,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS jobs_claim_idx
            ON jobs(queue_name, state, available_at, id);
        CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(state, lease_until);

        CREATE TABLE IF NOT EXISTS job_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            from_state TEXT,
            to_state TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            brand TEXT NOT NULL,
            main_image TEXT NOT NULL,
            images TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            features TEXT NOT NULL,
            specs TEXT NOT NULL,
            reviews TEXT NOT NULL DEFAULT '[]',
            category_path TEXT NOT NULL,
            category_level_1 TEXT NOT NULL,
            category_level_2 TEXT NOT NULL,
            category_level_3 TEXT NOT NULL,
            category_leaf TEXT NOT NULL,
            category_node_ids TEXT NOT NULL,
            category_source TEXT NOT NULL,
            category_complete TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS variants (
            item_number TEXT PRIMARY KEY,
            product_id TEXT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
            product_group_id TEXT NOT NULL,
            color TEXT NOT NULL DEFAULT '',
            capacity TEXT NOT NULL DEFAULT '',
            material TEXT NOT NULL DEFAULT '',
            selling_price TEXT NOT NULL,
            currency TEXT NOT NULL,
            variation_theme TEXT NOT NULL,
            variation_value TEXT NOT NULL,
            category_key TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS variants_product_idx ON variants(product_id);
        CREATE INDEX IF NOT EXISTS variants_group_idx ON variants(product_group_id);
        CREATE INDEX IF NOT EXISTS variants_category_idx ON variants(category_key);

        CREATE TABLE IF NOT EXISTS sellers (
            seller_id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            official_name TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS offers (
            listing_id TEXT PRIMARY KEY,
            item_number TEXT NOT NULL REFERENCES variants(item_number) ON DELETE CASCADE,
            seller_id TEXT REFERENCES sellers(seller_id),
            selling_price TEXT NOT NULL,
            currency TEXT NOT NULL,
            in_stock INTEGER,
            scraped_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS offers_item_idx ON offers(item_number);

        CREATE TABLE IF NOT EXISTS reviews (
            review_key TEXT PRIMARY KEY,
            product_id TEXT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
            stars REAL NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL DEFAULT '',
            review_date TEXT NOT NULL DEFAULT '',
            author TEXT NOT NULL DEFAULT '',
            images TEXT NOT NULL DEFAULT '[]',
            position INTEGER NOT NULL DEFAULT 0,
            scraped_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS reviews_product_idx ON reviews(product_id);
        """)
        review_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(reviews)").fetchall()
        }
        if "position" not in review_columns:
            connection.execute(
                "ALTER TABLE reviews ADD COLUMN position INTEGER NOT NULL DEFAULT 0"
            )
        connection.execute(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES('schema_version','2')"
        )
        connection.commit()

    @staticmethod
    def _event(connection, job_id, from_state, to_state, queue_name, reason=""):
        connection.execute(
            "INSERT INTO job_events(job_id,from_state,to_state,queue_name,reason,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (job_id, from_state, to_state, queue_name, reason, utc_now()),
        )

    def enqueue_many(self, candidates):
        connection = self.connection()
        now = utc_now()
        before = connection.total_changes
        connection.executemany(
            "INSERT OR IGNORE INTO jobs(product_id,path,url,state,queue_name,available_at,"
            "discovered_at,updated_at) VALUES(?,?,?,'pending','direct',0,?,?)",
            ((str(product_id), path, url, now, now) for path, product_id, url in candidates),
        )
        connection.commit()
        return connection.total_changes - before

    def recover_stale(self):
        connection = self.connection()
        now_epoch, now_text = time.time(), utc_now()
        with immediate(connection):
            rows = connection.execute(
                "SELECT id,queue_name FROM jobs WHERE state='processing' "
                "AND (lease_until IS NULL OR lease_until<?)",
                (now_epoch,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE jobs SET state='retry',available_at=?,lease_until=NULL,worker_id=NULL,"
                    "last_error='stale_lease_recovered',updated_at=? WHERE id=?",
                    (now_epoch, now_text, row["id"]),
                )
                self._event(
                    connection, row["id"], "processing", "retry",
                    row["queue_name"], "stale_lease_recovered",
                )
        return len(rows)

    def claim(self, queue_name, worker_id, lease_seconds=180):
        connection = self.connection()
        now_epoch, now_text = time.time(), utc_now()
        with immediate(connection):
            row = connection.execute(
                "SELECT * FROM jobs WHERE queue_name=? AND state IN ('pending','retry') "
                "AND available_at<=? ORDER BY available_at,id LIMIT 1",
                (queue_name, now_epoch),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                "UPDATE jobs SET state='processing',attempts=attempts+1,lease_until=?,"
                "worker_id=?,updated_at=? WHERE id=?",
                (now_epoch + lease_seconds, worker_id, now_text, row["id"]),
            )
            self._event(
                connection, row["id"], row["state"], "processing", queue_name, worker_id
            )
            result = dict(row)
            result.update({
                "state": "processing",
                "attempts": row["attempts"] + 1,
                "worker_id": worker_id,
                "lease_until": now_epoch + lease_seconds,
            })
            return result

    def _transition(self, job_id, to_state, reason="", http_status=None,
                    queue_name=None, available_at=0):
        connection = self.connection()
        with immediate(connection):
            row = connection.execute("SELECT state,queue_name FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return
            target_queue = queue_name or row["queue_name"]
            connection.execute(
                "UPDATE jobs SET state=?,queue_name=?,available_at=?,lease_until=NULL,"
                "worker_id=NULL,last_error=?,http_status=?,updated_at=? WHERE id=?",
                (to_state, target_queue, available_at, reason, http_status, utc_now(), job_id),
            )
            self._event(
                connection, job_id, row["state"], to_state, target_queue, reason
            )

    def move_to_cloudbypass(self, job_id, reason, http_status=None):
        self._transition(
            job_id, "pending", reason, http_status,
            queue_name="cloudbypass", available_at=time.time(),
        )

    def retry(self, job, reason, http_status=None, max_attempts=6,
              base_delay=5, max_delay=900):
        attempts = int(job.get("attempts", 1))
        if attempts >= max_attempts:
            self.skip(job["id"], f"max_attempts:{reason}", http_status)
            return "skip"
        delay = min(max_delay, base_delay * (2 ** max(attempts - 1, 0)))
        delay += random.uniform(0, base_delay)
        self._transition(
            job["id"], "retry", reason, http_status,
            queue_name=job["queue_name"], available_at=time.time() + delay,
        )
        return "retry"

    def complete(self, job_id):
        self._transition(job_id, "done")

    def skip(self, job_id, reason, http_status=None):
        self._transition(job_id, "skip", reason, http_status)

    def release_worker(self, worker_id):
        connection = self.connection()
        now_epoch, now_text = time.time(), utc_now()
        with immediate(connection):
            rows = connection.execute(
                "SELECT id,queue_name FROM jobs WHERE state='processing' AND worker_id=?",
                (worker_id,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE jobs SET state='retry',available_at=?,lease_until=NULL,worker_id=NULL,"
                    "last_error='worker_shutdown',updated_at=? WHERE id=?",
                    (now_epoch, now_text, row["id"]),
                )
                self._event(
                    connection, row["id"], "processing", "retry",
                    row["queue_name"], "worker_shutdown",
                )

    def queue_counts(self):
        rows = self.connection().execute(
            "SELECT queue_name,state,COUNT(*) AS count FROM jobs GROUP BY queue_name,state"
        ).fetchall()
        return {(row["queue_name"], row["state"]): row["count"] for row in rows}

    def active_jobs(self):
        row = self.connection().execute(
            "SELECT COUNT(*) AS count FROM jobs WHERE state IN ('pending','processing','retry')"
        ).fetchone()
        return row["count"]

    def variant_count(self):
        return self.connection().execute("SELECT COUNT(*) FROM variants").fetchone()[0]

    def next_available_at(self):
        row = self.connection().execute(
            "SELECT MIN(available_at) FROM jobs WHERE state IN ('pending','retry')"
        ).fetchone()
        return row[0]

    def status_snapshot(self):
        counts = {
            queue_name: {state: 0 for state in JOB_STATES}
            for queue_name in QUEUE_NAMES
        }
        for (queue_name, state), count in self.queue_counts().items():
            counts[queue_name][state] = count
        connection = self.connection()
        tables = {}
        for table in ("products", "variants", "sellers", "offers", "reviews"):
            tables[table] = connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        return {"queues": counts, "tables": tables}

    @staticmethod
    def _category_key(record):
        node_ids = record.get("category_node_ids", "")
        return node_ids.split("|")[-1] if node_ids else record.get("category_path", "")

    def persist_product(self, records, reviews, commerce, total, category_cap):
        if not records:
            return 0
        connection = self.connection()
        now = utc_now()
        inserted = 0
        with immediate(connection):
            current_total = connection.execute("SELECT COUNT(*) FROM variants").fetchone()[0]
            category_counts = {}
            accepted = []
            seen_items = set()
            for record in records:
                item_number = record["item_number"]
                if item_number in seen_items:
                    continue
                seen_items.add(item_number)
                exists = connection.execute(
                    "SELECT 1 FROM variants WHERE item_number=?", (item_number,)
                ).fetchone()
                if not exists:
                    if total > 0 and current_total >= total:
                        break
                    category_key = self._category_key(record)
                    if category_cap > 0:
                        count = category_counts.get(category_key)
                        if count is None:
                            count = connection.execute(
                                "SELECT COUNT(*) FROM variants WHERE category_key=?", (category_key,)
                            ).fetchone()[0]
                        if count >= category_cap:
                            continue
                        category_counts[category_key] = count + 1
                    current_total += 1
                accepted.append((record, bool(exists)))

            for record, exists in accepted:
                connection.execute("""
                    INSERT INTO products(
                        product_id,url,title,brand,main_image,images,description,features,specs,reviews,
                        category_path,category_level_1,category_level_2,category_level_3,category_leaf,
                        category_node_ids,category_source,category_complete,scraped_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(product_id) DO UPDATE SET
                        url=excluded.url,title=excluded.title,brand=excluded.brand,
                        main_image=excluded.main_image,images=excluded.images,
                        features=excluded.features,specs=excluded.specs,
                        category_path=excluded.category_path,category_level_1=excluded.category_level_1,
                        category_level_2=excluded.category_level_2,category_level_3=excluded.category_level_3,
                        category_leaf=excluded.category_leaf,category_node_ids=excluded.category_node_ids,
                        category_source=excluded.category_source,category_complete=excluded.category_complete,
                        scraped_at=excluded.scraped_at,updated_at=excluded.updated_at
                """, (
                    record["product_id"], record["url"], record["title"], record["brand"],
                    record["main_image"], record["images"], record["description"], record["features"],
                    record["specs"], "[]", record["category_path"],
                    record["category_level_1"], record["category_level_2"], record["category_level_3"],
                    record["category_leaf"], record["category_node_ids"], record["category_source"],
                    record["category_complete"], record["scraped_at"], now,
                ))
                connection.execute("""
                    INSERT INTO variants(
                        item_number,product_id,product_group_id,color,capacity,material,
                        selling_price,currency,variation_theme,variation_value,category_key,
                        scraped_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(item_number) DO UPDATE SET
                        product_id=excluded.product_id,product_group_id=excluded.product_group_id,
                        color=excluded.color,capacity=excluded.capacity,material=excluded.material,
                        selling_price=excluded.selling_price,currency=excluded.currency,
                        variation_theme=excluded.variation_theme,variation_value=excluded.variation_value,
                        category_key=excluded.category_key,scraped_at=excluded.scraped_at,
                        updated_at=excluded.updated_at
                """, (
                    record["item_number"], record["product_id"], record["product_group_id"],
                    record["color"], record["capacity"], record["material"],
                    record["selling_price"], record["currency"], record["variation_theme"],
                    record["variation_value"], self._category_key(record), record["scraped_at"], now,
                ))
                if not exists:
                    inserted += 1

            if accepted:
                product_id = accepted[0][0]["product_id"]
                connection.execute("DELETE FROM reviews WHERE product_id=?", (product_id,))
                for position, review in enumerate((reviews or [])[:10]):
                    raw_key = "\x1f".join(str(review.get(key, "")) for key in (
                        "body", "title", "date", "author"
                    ))
                    review_key = hashlib.sha256((product_id + "\x1f" + raw_key).encode("utf-8")).hexdigest()
                    connection.execute(
                        "INSERT OR REPLACE INTO reviews(review_key,product_id,stars,title,body,"
                        "review_date,author,images,position,scraped_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            review_key, product_id, review["stars"], review.get("title", ""),
                            review.get("body", ""), review.get("date", ""), review.get("author", ""),
                            json.dumps(review.get("images", []), ensure_ascii=False),
                            position,
                            accepted[0][0]["scraped_at"],
                        ),
                    )

                seller = (commerce or {}).get("seller") or {}
                seller_id = str(seller.get("seller_id") or "")
                if seller_id:
                    connection.execute(
                        "INSERT INTO sellers(seller_id,name,official_name,updated_at) VALUES(?,?,?,?) "
                        "ON CONFLICT(seller_id) DO UPDATE SET name=excluded.name,"
                        "official_name=excluded.official_name,updated_at=excluded.updated_at",
                        (seller_id, seller.get("name", ""), seller.get("official_name", ""), now),
                    )
                for offer in (commerce or {}).get("offers") or []:
                    exists_variant = connection.execute(
                        "SELECT 1 FROM variants WHERE item_number=?", (offer["item_number"],)
                    ).fetchone()
                    if not offer.get("listing_id") or not exists_variant:
                        continue
                    connection.execute("""
                        INSERT INTO offers(listing_id,item_number,seller_id,selling_price,currency,
                            in_stock,scraped_at,updated_at) VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(listing_id) DO UPDATE SET
                            item_number=excluded.item_number,seller_id=excluded.seller_id,
                            selling_price=excluded.selling_price,currency=excluded.currency,
                            in_stock=excluded.in_stock,scraped_at=excluded.scraped_at,
                            updated_at=excluded.updated_at
                    """, (
                        offer["listing_id"], offer["item_number"], seller_id or None,
                        offer.get("selling_price", ""), offer.get("currency", "TRY"),
                        offer.get("in_stock"), accepted[0][0]["scraped_at"], now,
                    ))
        return inserted

    def iter_export_rows(self):
        cursor = self.connection().execute("""
            SELECT
                p.product_id,v.item_number,p.url,p.title,p.brand,
                v.color,v.capacity,v.material,v.selling_price,v.currency,
                p.main_image,p.images,p.description,p.features,p.specs,
                v.product_group_id,v.variation_theme,v.variation_value,v.scraped_at,
                'ok' AS status,'' AS error,p.category_path,p.category_level_1,
                p.category_level_2,p.category_level_3,p.category_leaf,p.category_node_ids,
                p.category_source,p.category_complete
            FROM variants v JOIN products p ON p.product_id=v.product_id
            ORDER BY p.product_id,v.item_number
        """)
        for row in cursor:
            result = dict(row)
            review_rows = self.connection().execute(
                "SELECT stars,title,body,review_date,author,images FROM reviews "
                "WHERE product_id=? ORDER BY position,review_key LIMIT 10",
                (result["product_id"],),
            ).fetchall()
            reviews = []
            for review in review_rows:
                stars = review["stars"]
                if float(stars).is_integer():
                    stars = int(stars)
                reviews.append({
                    "stars": stars,
                    "title": review["title"],
                    "body": review["body"],
                    "date": review["review_date"],
                    "author": review["author"],
                    "images": json.loads(review["images"]),
                })
            result["reviews"] = json.dumps(
                reviews, ensure_ascii=False, separators=(",", ":")
            )
            yield result
