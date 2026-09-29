# -*- coding: utf-8 -*-
import csv
import json
import os
import tempfile
import threading
import time
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest import mock

import pipeline
import sqlite_store
from pipeline import export_csv
from sqlite_store import SQLiteStore
from trendyol_scraper import CSV_FIELDS, build_records


def ecommerce_state():
    return {"product": {
        "id": 881465224,
        "productGroupId": 673431937,
        "name": "Örnek Telefon",
        "brand": {"name": "Demo"},
        "images": ["//cdn.example/main.jpg", "//cdn.example/side.jpg"],
        "attributes": [
            {"key": {"name": "Renk"}, "value": {"name": "Siyah"}},
            {"key": {"name": "Materyal"}, "value": {"name": "Alüminyum"}},
        ],
        "slicingAttributes": {"Internal Memory": "128 GB", "DsmColor": "Siyah"},
        "variantAttributeName": "Internal Memory",
        "categoryTree": [
            {"id": 1, "name": "Elektronik"},
            {"id": 2, "name": "Telefon"},
            {"id": 3, "name": "Akıllı Telefon"},
        ],
        "variants": [
            {"itemNumber": 1221077500, "value": "128 GB", "price": {"value": 6000}},
            {"itemNumber": 1221077501, "value": "256 GB", "price": {"value": 7000}},
        ],
        "merchantListing": {
            "merchant": {"id": 42, "name": "Demo Satıcı", "officialName": "Demo A.Ş."},
            "variants": [
                {"itemNumber": 1221077500, "listingId": "listing-1", "inStock": True,
                 "price": {"value": 6000}},
                {"itemNumber": 1221077501, "listingId": "listing-2", "inStock": True,
                 "price": {"value": 7000}},
            ],
            "winnerVariant": {
                "itemNumber": 1221077500,
                "listingId": "listing-1",
                "inStock": True,
                "price": {"currency": "TRY", "sellingPrice": {"value": 6000}},
            },
        },
    }}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "state", "trendyol.db")
        self.store = SQLiteStore(self.db_path)
        self.store.initialize()

    def tearDown(self):
        self.store.close_thread()
        self.temp.cleanup()

    def enqueue(self, product_id="881465224"):
        path = f"/ornek-p-{product_id}"
        self.store.enqueue_many([(path, product_id, "https://www.trendyol.com" + path)])

    def test_wal_enqueue_dedup_and_transactional_claim(self):
        mode = self.store.connection().execute("PRAGMA journal_mode").fetchone()[0]
        self.assertEqual(mode.casefold(), "wal")
        candidate = ("/ornek-p-881465224", "881465224", "https://www.trendyol.com/ornek-p-881465224")
        self.assertEqual(self.store.enqueue_many([candidate, candidate]), 1)

        barrier = threading.Barrier(2)
        claimed = []

        def compete(worker_id):
            barrier.wait()
            job = self.store.claim("direct", worker_id, lease_seconds=30)
            if job:
                claimed.append(job["id"])
                self.store.release_worker(worker_id)
            self.store.close_thread()

        threads = [threading.Thread(target=compete, args=(f"w{index}",)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(claimed), 1)

    def test_retry_is_durable_and_expired_lease_recovers(self):
        self.enqueue("881465224")
        job = self.store.claim("direct", "retry-worker")
        before = time.time()
        with mock.patch.object(sqlite_store.random, "uniform", return_value=0):
            self.assertEqual(
                self.store.retry(job, "rate_limited", 429, max_attempts=5, base_delay=5),
                "retry",
            )
        row = self.store.connection().execute(
            "SELECT state,available_at,http_status FROM jobs WHERE id=?", (job["id"],)
        ).fetchone()
        self.assertEqual((row["state"], row["http_status"]), ("retry", 429))
        self.assertGreaterEqual(row["available_at"], before + 5)

        self.enqueue("881465225")
        stale = self.store.claim("direct", "stale-worker", lease_seconds=-1)
        self.assertIsNotNone(stale)
        self.assertEqual(self.store.recover_stale(), 1)
        recovered = self.store.connection().execute(
            "SELECT state,last_error FROM jobs WHERE id=?", (stale["id"],)
        ).fetchone()
        self.assertEqual(tuple(recovered), ("retry", "stale_lease_recovered"))

    def test_normalized_persistence_restart_and_streaming_export(self):
        reviews = [{
            "stars": 5,
            "title": "",
            "body": "Çok iyi",
            "date": "2026-08-22",
            "author": "Ali",
            "images": ["https://cdn.example/review.jpg"],
        }]
        state = ecommerce_state()
        records = build_records(
            state,
            "/ornek-telefon-p-881465224",
            scraped_at="2026-08-22T10:00:00Z",
            reviews=reviews,
        )
        commerce = pipeline.build_commerce(state, records)
        self.assertEqual(self.store.persist_product(records, reviews, commerce, 100, 10), 2)
        snapshot = self.store.status_snapshot()["tables"]
        self.assertEqual(snapshot, {
            "products": 1,
            "variants": 2,
            "sellers": 1,
            "offers": 2,
            "reviews": 1,
        })
        rows = list(self.store.iter_export_rows())
        self.assertEqual({row["item_number"] for row in rows}, {"1221077500", "1221077501"})
        self.assertEqual({row["product_group_id"] for row in rows}, {"673431937"})
        self.assertEqual({row["capacity"] for row in rows}, {"128 GB", "256 GB"})
        self.assertTrue(all(row["description"] == "" for row in rows))
        self.assertTrue(all(set(("color", "capacity", "material")) <= set(row) for row in rows))

        output = os.path.join(self.temp.name, "out", "TRENDYOL_SKU_DATA.csv")
        self.assertEqual(export_csv(self.store, output), 2)
        with open(output, encoding="utf-8", newline="") as handle:
            exported = list(csv.DictReader(handle))
        self.assertEqual(list(exported[0]), CSV_FIELDS)
        self.assertEqual(len(exported), 2)
        self.assertEqual(len(json.loads(exported[0]["reviews"])), 1)

        self.store.close_thread()
        resumed = SQLiteStore(self.db_path)
        resumed.initialize()
        try:
            self.assertEqual(resumed.variant_count(), 2)
            self.assertEqual(len(list(resumed.iter_export_rows())), 2)
        finally:
            resumed.close_thread()


class PipelineClassificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(os.path.join(self.temp.name, "trendyol.db"))
        self.store.initialize()
        self.settings = SimpleNamespace(request_timeout=2, max_attempts=4)

    def tearDown(self):
        self.store.close_thread()
        self.temp.cleanup()

    def claim(self, product_id):
        path = f"/ornek-p-{product_id}"
        self.store.enqueue_many([(path, product_id, "https://www.trendyol.com" + path)])
        return self.store.claim("direct", "classification-worker")

    def test_challenge_moves_to_cloudbypass_and_404_skips(self):
        challenge = SimpleNamespace(status_code=403, text="Attention Required | cf-chl")
        session = SimpleNamespace(get=mock.Mock(return_value=challenge))
        first = self.claim("881465224")
        with mock.patch.object(pipeline, "get_direct_http_session", return_value=session):
            pipeline.handle_direct_job(self.store, first, self.settings)
        row = self.store.connection().execute(
            "SELECT state,queue_name,last_error FROM jobs WHERE id=?", (first["id"],)
        ).fetchone()
        self.assertEqual(tuple(row), ("pending", "cloudbypass", "challenge"))

        missing = SimpleNamespace(status_code=404, text="not found")
        session.get.return_value = missing
        second = self.claim("881465225")
        with mock.patch.object(pipeline, "get_direct_http_session", return_value=session):
            pipeline.handle_direct_job(self.store, second, self.settings)
        row = self.store.connection().execute(
            "SELECT state,queue_name,last_error FROM jobs WHERE id=?", (second["id"],)
        ).fetchone()
        self.assertEqual(tuple(row), ("skip", "direct", "not_found"))

    def test_candidate_discovery_deduplicates_product_links(self):
        page = """
        <a href="/telefon/model-p-881465224?boutiqueId=1">A</a>
        <a data-href="https://www.trendyol.com/telefon/model-p-881465224">A2</a>
        <a href="/tablet/model-p-881465225">B</a>
        """
        candidates = pipeline.extract_product_candidates(page)
        self.assertEqual([candidate[1] for candidate in candidates], ["881465224", "881465225"])
        self.assertEqual(candidates[0][0], "/telefon/model-p-881465224")

    def test_official_pipeline_has_no_component_read_request(self):
        for filename in ("pipeline.py", "trendyol_scraper.py"):
            with open(os.path.join(os.path.dirname(__file__), filename), encoding="utf-8") as handle:
                source = handle.read().casefold()
            self.assertNotIn("component-read", source)


if __name__ == "__main__":
    unittest.main()
