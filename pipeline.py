# -*- coding: utf-8 -*-
"""Durable Trendyol discovery, crawling, storage, and CSV export pipeline."""
import argparse
import csv
import html
import json
import os
import re
import tempfile
import threading
import time
import uuid
from decimal import Decimal
from urllib.parse import urlsplit

from sqlite_store import SQLiteStore
from trendyol_scraper import (
    CSV_FIELDS,
    DEFAULT_EXCLUDED_CATEGORY_TERMS,
    DEFAULT_MIN_SELLING_PRICE,
    REQUEST_EXCEPTION,
    UA,
    _product,
    build_records,
    cb_get,
    clean_text,
    close_http_session,
    env,
    extract_page_reviews,
    extract_state,
    fetch_sitemap,
    get_direct_http_session,
    is_excluded_category,
    require_runtime_config,
    validate_record,
)


DEFAULT_DB = os.path.join("state", "trendyol.db")
DEFAULT_OUTPUT = os.path.join("out", "TRENDYOL_SKU_DATA.csv")
PRODUCT_PATTERN = re.compile(r"(/[^\s\"'<>]*?-p-(\d{4,})(?:[/?][^\s\"'<>]*)?)", re.I)
CHALLENGE_MARKERS = (
    "attention required",
    "cf-chl",
    "verify you are human",
    "cloudflare ray id",
    "challenge-platform",
)
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
TERMINAL_HTTP = {404, 410}


def product_candidate(value):
    """Return (path, product_id, canonical_url) for a Trendyol product URL/path."""
    value = html.unescape(str(value or "").strip())
    if not value:
        return None
    if value.startswith("//"):
        value = "https:" + value
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlsplit(value)
        if not parsed.netloc.casefold().endswith("trendyol.com"):
            return None
        value = parsed.path
    elif not value.startswith("/"):
        value = "/" + value
    match = PRODUCT_PATTERN.search(value)
    if not match:
        return None
    path = match.group(1).split("?", 1)[0]
    product_id = match.group(2)
    return path, product_id, "https://www.trendyol.com" + path


def extract_product_candidates(page_html):
    seen = set()
    candidates = []
    for raw in re.findall(r"(?:href|data-href)\s*=\s*[\"']([^\"']+)", page_html or "", re.I):
        candidate = product_candidate(raw)
        if candidate and candidate[1] not in seen:
            seen.add(candidate[1])
            candidates.append(candidate)
    return candidates


def is_challenge_page(page_html):
    sample = (page_html or "")[:200000].casefold()
    if any(marker in sample for marker in CHALLENGE_MARKERS):
        return True
    return bool(re.search(r"<title[^>]*>[^<]*captcha|(?:id|class)=[\"'][^\"']*captcha", sample))


def discover_sitemaps(store, sitemap_count):
    discovered = 0
    for number in range(1, max(0, sitemap_count) + 1):
        candidates = [
            (path, product_id, "https://www.trendyol.com" + path)
            for path, product_id in fetch_sitemap(number)
        ]
        added = store.enqueue_many(candidates)
        discovered += added
        print(f"发现 sitemap {number}/{sitemap_count}: 候选 {len(candidates)}，新增 {added}", flush=True)
    return discovered


def discover_seed_file(store, seed_file, timeout=45):
    if not seed_file:
        return 0
    with open(seed_file, encoding="utf-8-sig") as handle:
        seeds = [line.strip() for line in handle if line.strip() and not line.lstrip().startswith("#")]
    direct, listing_pages = [], []
    for seed in seeds:
        candidate = product_candidate(seed)
        if candidate:
            direct.append(candidate)
        else:
            listing_pages.append(seed)
    added = store.enqueue_many(direct)
    for seed in listing_pages:
        url = seed if seed.startswith(("http://", "https://")) else "https://www.trendyol.com/" + seed.lstrip("/")
        try:
            response = get_direct_http_session().get(
                url,
                headers={"user-agent": UA, "accept-language": "tr-TR,tr;q=0.9"},
                timeout=timeout,
            )
            if response.status_code != 200 or is_challenge_page(response.text):
                print(f"种子页未发现商品: HTTP {response.status_code} {url}", flush=True)
                continue
            candidates = extract_product_candidates(response.text)
            page_added = store.enqueue_many(candidates)
            added += page_added
            print(f"发现种子页: 候选 {len(candidates)}，新增 {page_added} {url}", flush=True)
        except REQUEST_EXCEPTION as exc:
            print(f"种子页请求失败: {type(exc).__name__} {url}", flush=True)
    return added


def build_commerce(state, records):
    """Normalize seller/offer facts already present in the detail response only."""
    product = _product(state)
    listing = product.get("merchantListing") or {}
    merchant = listing.get("merchant") or product.get("merchant") or {}
    seller_id = clean_text(
        merchant.get("id") or merchant.get("merchantId") or merchant.get("sellerId")
    )
    seller = {
        "seller_id": seller_id,
        "name": clean_text(merchant.get("name") or merchant.get("sellerName")),
        "official_name": clean_text(merchant.get("officialName")),
    }
    record_by_item = {record["item_number"]: record for record in records}
    winner = listing.get("winnerVariant") or {}
    sources = list(listing.get("variants") or []) + [winner]
    offers, seen = [], set()
    for variant in sources:
        if not isinstance(variant, dict):
            continue
        listing_id = clean_text(variant.get("listingId"))
        item_number = clean_text(variant.get("itemNumber"))
        if not item_number and len(record_by_item) == 1:
            item_number = next(iter(record_by_item))
        record = record_by_item.get(item_number)
        if not listing_id or not record or listing_id in seen:
            continue
        seen.add(listing_id)
        offers.append({
            "listing_id": listing_id,
            "item_number": item_number,
            "selling_price": record["selling_price"],
            "currency": record["currency"],
            # Stock is intentionally outside the requested collection scope.
            "in_stock": None,
        })
    return {"seller": seller, "offers": offers}


def parse_detail(page_html, path, excluded_terms, min_selling_price):
    state = extract_state(page_html)
    if not state:
        return None, None, None, "missing_product_state"
    reviews = extract_page_reviews(state, page_html, limit=10)
    records = build_records(state, path, reviews=reviews)
    eligible, reasons = [], []
    for record in records:
        if is_excluded_category(record, excluded_terms):
            reasons.append("excluded_category")
            continue
        errors = validate_record(record, min_selling_price)
        if errors:
            reasons.extend(errors)
            continue
        eligible.append(record)
    if not eligible:
        reason = ",".join(sorted(set(reasons))) or "no_variants"
        return [], reviews, {}, reason
    return eligible, reviews, build_commerce(state, eligible), ""


def persist_detail(store, job, page_html, settings, bypass=False):
    records, reviews, commerce, reason = parse_detail(
        page_html,
        job["path"],
        settings.excluded_terms,
        settings.min_selling_price,
    )
    if records is None:
        if bypass:
            store.retry(job, reason, max_attempts=settings.max_attempts)
        else:
            store.move_to_cloudbypass(job["id"], reason, 200)
        return
    if not records:
        store.skip(job["id"], reason, 200)
        return
    store.persist_product(records, reviews, commerce, settings.total, settings.category_cap)
    store.complete(job["id"])


def handle_direct_job(store, job, settings):
    try:
        response = get_direct_http_session().get(
            job["url"],
            headers={
                "user-agent": UA,
                "accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "accept-language": "tr-TR,tr;q=0.9",
            },
            timeout=settings.request_timeout,
        )
    except REQUEST_EXCEPTION as exc:
        store.retry(job, f"network:{type(exc).__name__}", max_attempts=settings.max_attempts)
        return
    status, page_html = response.status_code, response.text
    if is_challenge_page(page_html) or status in (401, 403):
        store.move_to_cloudbypass(job["id"], "challenge", status)
    elif status in TERMINAL_HTTP:
        store.skip(job["id"], "not_found", status)
    elif status in RETRYABLE_HTTP or status >= 500:
        store.retry(job, f"http_{status}", status, settings.max_attempts)
    elif 400 <= status < 500:
        store.skip(job["id"], f"terminal_http_{status}", status)
    elif status != 200:
        store.retry(job, f"unexpected_http_{status}", status, settings.max_attempts)
    else:
        persist_detail(store, job, page_html, settings, bypass=False)


def handle_cloudbypass_job(store, job, settings):
    status, page_html = cb_get(job["path"], "www.trendyol.com", 2, tries=1, cost_key="page")
    if status in TERMINAL_HTTP:
        store.skip(job["id"], "not_found", status)
    elif status != 200 or not page_html:
        store.retry(job, "cloudbypass_failed", status, settings.max_attempts)
    else:
        persist_detail(store, job, page_html, settings, bypass=True)


def worker_loop(store, queue_name, worker_id, settings, stop_event):
    handler = handle_direct_job if queue_name == "direct" else handle_cloudbypass_job
    try:
        while not stop_event.is_set():
            job = store.claim(queue_name, worker_id, settings.lease_seconds)
            if job is None:
                stop_event.wait(0.25)
                continue
            try:
                handler(store, job, settings)
            except Exception as exc:
                store.retry(
                    job,
                    f"worker_error:{type(exc).__name__}:{str(exc)[:300]}",
                    max_attempts=settings.max_attempts,
                )
    finally:
        store.release_worker(worker_id)
        store.close_thread()
        close_http_session()


def run_workers(store, settings):
    if settings.workers < 1 or settings.bypass_workers < 1:
        raise SystemExit("run 模式要求 --workers 和 --bypass-workers 都至少为 1")
    stop_event = threading.Event()
    threads = []
    run_id = uuid.uuid4().hex[:10]
    for queue_name, count in (("direct", settings.workers), ("cloudbypass", settings.bypass_workers)):
        for index in range(count):
            worker_id = f"{run_id}:{queue_name}:{index + 1}"
            thread = threading.Thread(
                target=worker_loop,
                args=(store, queue_name, worker_id, settings, stop_event),
                name=worker_id,
            )
            thread.start()
            threads.append(thread)
    last_report = 0
    try:
        while True:
            active = store.active_jobs()
            delivered = store.variant_count()
            now = time.time()
            if now - last_report >= settings.status_interval:
                print(f"进度: 活跃任务 {active}，已入库 SKU {delivered}/{settings.total}", flush=True)
                last_report = now
            if active == 0 or (settings.total > 0 and delivered >= settings.total):
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("收到停止信号，正在释放 Worker 租约", flush=True)
    finally:
        stop_event.set()
        for thread in threads:
            thread.join()


def export_csv(store, output_path, min_selling_price=DEFAULT_MIN_SELLING_PRICE):
    """Stream a WAL snapshot to a temp file, validate it, then atomically publish."""
    output_path = os.path.abspath(output_path)
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".trendyol-export-", suffix=".csv", dir=output_dir)
    count, seen = 0, set()
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="raise")
            writer.writeheader()
            for row in store.iter_export_rows():
                errors = validate_record(row, min_selling_price)
                if row["item_number"] in seen:
                    errors.append("duplicate_item_number")
                if errors:
                    raise ValueError(f"SKU {row.get('item_number')}: {';'.join(errors)}")
                seen.add(row["item_number"])
                writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, output_path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise
    print(f"导出完成: {count} 个 SKU -> {output_path}", flush=True)
    return count


def print_status(store, as_json=False):
    snapshot = store.status_snapshot()
    if as_json:
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True))
        return
    for queue_name in ("direct", "cloudbypass"):
        counts = snapshot["queues"][queue_name]
        detail = " ".join(f"{state}={counts[state]}" for state in ("pending", "processing", "retry", "done", "skip"))
        print(f"{queue_name}: {detail}")
    print("数据表: " + " ".join(f"{name}={count}" for name, count in snapshot["tables"].items()))


def build_parser():
    parser = argparse.ArgumentParser(description="Trendyol SQLite WAL 持久化采集流水线")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def common(command):
        command.add_argument("--db", default=DEFAULT_DB)
        command.add_argument("--output", default=DEFAULT_OUTPUT)
        command.add_argument("--total", type=int, default=300000)
        command.add_argument("--cap", dest="category_cap", type=int, default=90)
        command.add_argument("--min-price", dest="min_selling_price", type=Decimal, default=DEFAULT_MIN_SELLING_PRICE)
        command.add_argument(
            "--excluded",
            default="|".join(DEFAULT_EXCLUDED_CATEGORY_TERMS),
            help="以 | 分隔的分类排除词",
        )

    init = subparsers.add_parser("init", help="初始化 WAL 数据库")
    init.add_argument("--db", default=DEFAULT_DB)
    status = subparsers.add_parser("status", help="查看持久化队列和数据表计数")
    status.add_argument("--db", default=DEFAULT_DB)
    status.add_argument("--json", action="store_true")
    export = subparsers.add_parser("export", help="从数据库流式生成并校验 CSV")
    common(export)

    for name in ("discover", "run"):
        command = subparsers.add_parser(name, help="发现候选" if name == "discover" else "发现、抓取并导出")
        common(command)
        command.add_argument("--sitemaps", type=int, default=301)
        command.add_argument("--seed-file")
        command.add_argument("--request-timeout", type=float, default=45)
        if name == "run":
            command.add_argument("--workers", type=int, default=32)
            command.add_argument("--bypass-workers", type=int, default=4)
            command.add_argument("--lease-seconds", type=int, default=180)
            command.add_argument("--max-attempts", type=int, default=6)
            command.add_argument("--status-interval", type=float, default=15)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    store = SQLiteStore(args.db)
    store.initialize()
    if args.command == "init":
        print(f"SQLite WAL 已初始化: {store.path}")
        return
    if args.command == "status":
        print_status(store, args.json)
        return
    args.excluded_terms = tuple(term.strip() for term in args.excluded.split("|") if term.strip())
    if args.command == "export":
        export_csv(store, args.output, args.min_selling_price)
        return

    if args.seed_file and not (env("PROXY_USER") and env("PROXY_PASS") and env("PROXY_HOST")):
        raise SystemExit("使用分类页/搜索页种子文件时必须配置土耳其代理")
    if args.command == "run":
        require_runtime_config()
    recovered = store.recover_stale()
    if recovered:
        print(f"恢复过期 processing 租约: {recovered}", flush=True)
    discover_sitemaps(store, args.sitemaps)
    discover_seed_file(store, args.seed_file, args.request_timeout)
    if args.command == "discover":
        print_status(store)
        return
    run_workers(store, args)
    export_csv(store, args.output, args.min_selling_price)
    print_status(store)


if __name__ == "__main__":
    main()
