# -*- coding: utf-8 -*-
import csv
import json
import os
import tempfile
import threading
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest import mock

import trendyol_scraper as scraper
from trendyol_scraper import (
    CSV_FIELDS, CsvDataset, build_record, build_records, extract_page_reviews, fetch_product,
    is_excluded_category, validate_record,
)
from validate_output import validate_row


def fixture_state(slicing=True, jewelry=False):
    tree = [
        {"id": 1, "name": "Moda" if jewelry else "Elektronik"},
        {"id": 2, "name": "Takı" if jewelry else "Telefon"},
        {"id": 3, "name": "Akıllı Telefon"},
    ]
    return {"product": {
        "id": 881465224,
        "productGroupId": 673431937,
        "name": "Örnek Telefon, 128 GB",
        "brand": {"name": "Demo"},
        "images": ["//cdn.example/main.jpg", "//cdn.example/side.jpg"],
        "attributes": [
            {"key": {"name": "RAM"}, "value": {"name": "8 GB"}},
            {"key": {"name": "Renk"}, "value": {"name": "Siyah"}},
            {"key": {"name": "Materyal"}, "value": {"name": "Alüminyum"}},
        ],
        "slicingAttributes": {"Internal Memory": "128 GB", "DsmColor": "Siyah"} if slicing else {},
        "categoryTree": tree,
        "merchantListing": {
            "winnerVariant": {
                "itemNumber": 1221077500,
                "listingId": "listing",
                "inStock": True,
                "price": {"currency": "TRY", "sellingPrice": {"value": 10000}},
            }
        },
    }}


class MappingTests(unittest.TestCase):
    def test_sanitized_fixture_matches_migration_baseline(self):
        root = os.path.dirname(__file__)
        with open(
            os.path.join(root, "testdata", "fixtures", "single_sku.html"),
            encoding="utf-8",
        ) as handle:
            state = scraper.extract_state(handle.read())
        records = build_records(
            state,
            "/samsung/galaxy-a16-p-881465224",
            scraped_at="2026-08-22T10:00:00Z",
            reviews=[],
        )
        with open(
            os.path.join(root, "testdata", "baseline", "historical_single.csv"),
            encoding="utf-8",
            newline="",
        ) as handle:
            baseline = list(csv.DictReader(handle))
        self.assertEqual(records, baseline)

    def test_maps_fixed_schema(self):
        record = build_record(
            fixture_state(), "/ornek-telefon-p-881465224",
            scraped_at="2026-08-22T10:00:00Z",
        )
        self.assertEqual(list(record), CSV_FIELDS)
        self.assertEqual(len(record), 30)
        self.assertEqual(record["item_number"], "1221077500")
        self.assertEqual(record["color"], "Siyah")
        self.assertEqual(record["capacity"], "128 GB")
        self.assertEqual(record["material"], "Alüminyum")
        self.assertEqual(record["selling_price"], "10000.00")
        self.assertEqual(record["currency"], "TRY")
        self.assertEqual(record["main_image"], "https://cdn.example/main.jpg")
        self.assertEqual(json.loads(record["specs"])["RAM"], "8 GB")
        self.assertEqual(record["description"], "")
        self.assertEqual(record["product_group_id"], "673431937")
        self.assertEqual(record["variation_theme"], "Internal Memory|DsmColor")
        self.assertEqual(record["variation_value"], "128 GB|Siyah")
        self.assertEqual(record["category_leaf"], "Akıllı Telefon")
        self.assertEqual(validate_record(record), [])
        self.assertEqual(validate_row(record, 2, set()), [])

    def test_safe_standalone_variation_fallback(self):
        record = build_record(fixture_state(slicing=False))
        self.assertEqual(record["product_group_id"], record["product_id"])
        self.assertEqual(record["variation_theme"], "")
        self.assertEqual(record["variation_value"], "")

    def test_fixed_dimensions_allow_missing_page_values(self):
        state = fixture_state(slicing=False)
        state["product"]["attributes"] = [
            {"key": {"name": "RAM"}, "value": {"name": "8 GB"}},
        ]
        record = build_record(state)
        self.assertEqual(
            {field: record[field] for field in ("color", "capacity", "material")},
            {"color": "", "capacity": "", "material": ""},
        )
        self.assertEqual(validate_record(record), [])

    def test_expands_multiple_variants_into_distinct_sku_rows(self):
        state = fixture_state(slicing=False)
        state["product"]["variants"] = [
            {
                "itemNumber": 1221077500,
                "value": "S",
                "inStock": True,
                "price": {"value": 6000},
            },
            {
                "itemNumber": 1221077501,
                "value": "M",
                "inStock": True,
                "price": {"value": 7000},
            },
        ]
        records = build_records(state, "/ornek-telefon-p-881465224")
        self.assertEqual(len(records), 2)
        self.assertEqual({record["item_number"] for record in records}, {"1221077500", "1221077501"})
        self.assertEqual({record["product_group_id"] for record in records}, {"673431937"})
        self.assertEqual([record["variation_value"] for record in records], ["S", "M"])
        self.assertEqual([record["selling_price"] for record in records], ["10000.00", "7000.00"])

    def test_jewelry_exclusion(self):
        record = build_record(fixture_state(jewelry=True))
        self.assertTrue(is_excluded_category(record, ["mücevher", "takı"]))

    def test_price_must_be_strictly_above_5000_try(self):
        state = fixture_state()
        state["product"]["merchantListing"]["winnerVariant"]["price"]["sellingPrice"]["value"] = 5000
        record = build_record(state)
        self.assertIn("selling_price_not_above_minimum", validate_record(record))
        state["product"]["merchantListing"]["winnerVariant"]["price"]["sellingPrice"]["value"] = 5000.01
        self.assertEqual(validate_record(build_record(state)), [])

    def test_embedded_reviews_limit_and_no_padding(self):
        state = fixture_state()
        state["product"]["reviews"] = [
            {
                "rate": 5,
                "comment": f"Yorum {index}",
                "userFullName": f"Kullanıcı {index}",
                "commentDateISOtype": "2026-08-22",
                "mediaFiles": [{"url": f"//cdn.example/review-{index}.jpg"}],
            }
            for index in range(12)
        ]
        reviews = extract_page_reviews(state)
        self.assertEqual(len(reviews), 10)
        self.assertEqual(reviews[0]["stars"], 5)
        self.assertEqual(reviews[0]["images"], ["https://cdn.example/review-0.jpg"])
        record = build_record(state, reviews=reviews)
        self.assertEqual(len(json.loads(record["reviews"])), 10)

        html = '<script>{"reviews":[{"rate":4,"comment":"Tek yorum","userFullName":"Ali"}]}</script>'
        page_reviews = extract_page_reviews({}, html)
        self.assertEqual(len(page_reviews), 1)
        self.assertEqual(page_reviews[0]["body"], "Tek yorum")

    def test_csv_round_trip(self):
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                dataset = CsvDataset("out/data.csv", total=10, category_cap=2)
                record = build_record(fixture_state())
                self.assertTrue(dataset.add(record))
                self.assertNotIn(record["product_id"], dataset.done)
                dataset.mark_done(record["product_id"])
                self.assertIn(record["product_id"], dataset.done)
                dataset.close()
                with open("out/data.csv", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 1)
                self.assertEqual(list(rows[0]), CSV_FIELDS)
                self.assertEqual(rows[0]["item_number"], "1221077500")
                self.assertEqual(rows[0]["title"], "Örnek Telefon, 128 GB")
            finally:
                os.chdir(original)

    def test_fetch_product_only_requests_detail_page(self):
        dataset = SimpleNamespace(
            min_selling_price=Decimal("5000"),
            wants=lambda record: True,
        )
        with (
            mock.patch.object(scraper, "cb_get", return_value=(200, "detail html")) as cb_get,
            mock.patch.object(scraper, "extract_state", return_value=fixture_state()),
            mock.patch.object(scraper, "extract_page_reviews", return_value=[]),
        ):
            outcome, records, reason = fetch_product(
                "/ornek-telefon-p-881465224", "881465224", dataset, []
            )
        self.assertEqual((outcome, reason), ("ok", ""))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["description"], "")
        self.assertEqual(cb_get.call_count, 1)

    def test_each_thread_owns_separate_direct_and_control_sessions(self):
        class FakeAdapter:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeSession:
            def __init__(self):
                self.closed = False
                self.proxies = {}
            def mount(self, prefix, adapter):
                pass
            def close(self):
                self.closed = True

        fake_requests = SimpleNamespace(
            Session=FakeSession,
            adapters=SimpleNamespace(HTTPAdapter=FakeAdapter),
        )
        session_sets = []

        def use_session():
            direct = scraper.get_direct_http_session()
            direct_again = scraper.get_direct_http_session()
            control = scraper.get_control_http_session()
            control_again = scraper.get_control_http_session()
            session_sets.append((direct, direct_again, control, control_again))
            scraper.close_http_session()

        with (
            mock.patch.object(scraper, "requests", fake_requests),
            mock.patch.object(
                scraper,
                "get_session",
                side_effect=({"sid": "worker-1"}, {"sid": "worker-2"}),
            ),
            mock.patch.dict(os.environ, {
                "PROXY_USER": "user-{sid}",
                "PROXY_PASS": "p@ss:word",
                "PROXY_HOST": "tr.example:9000",
            }),
        ):
            threads = [threading.Thread(target=use_session) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(len(session_sets), 2)
        for direct, direct_again, control, control_again in session_sets:
            self.assertIs(direct, direct_again)
            self.assertIs(control, control_again)
            self.assertIsNot(direct, control)
            self.assertTrue(direct.closed)
            self.assertTrue(control.closed)
        self.assertIsNot(session_sets[0][0], session_sets[1][0])
        self.assertEqual(
            {sessions[0].proxies["https"] for sessions in session_sets},
            {
                "http://user-worker-1:p%40ss%3Aword@tr.example:9000",
                "http://user-worker-2:p%40ss%3Aword@tr.example:9000",
            },
        )


if __name__ == "__main__":
    unittest.main()
