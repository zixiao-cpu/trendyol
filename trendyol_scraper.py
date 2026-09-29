# -*- coding: utf-8 -*-
"""Trendyol 商品采集器，输出 30 列、每行一个 SKU 的 UTF-8 CSV。

数据来源仍然是 Trendyol：
  * sitemap_products{N}.xml 枚举商品 URL；
  * Cloudbypass V2 获取商品详情页内嵌状态。

交付结构沿用既有数据契约，但平台字段保留 Trendyol 原生语义：
  product_id       <- product.id
  selling_price    <- winnerVariant.price.sellingPrice.value（不换算）
  product_group_id <- 有可靠切片属性时使用 productGroupId，否则等于 product_id
  variation_*      <- slicingAttributes
  category_*       <- categoryTree

评论仅从当前详情页响应中提取；页面没有评论时 reviews 写 []，不会为了补齐评论
而访问独立评论接口。详情页内嵌评论最多保留 10 条，不足不补齐。
列表字段使用竖线连接，对象字段使用合法 JSON。
"""

import argparse
import csv
import html as html_lib
import json
import os
import queue
import random
import re
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.parse import quote

try:
    import requests
except ModuleNotFoundError:  # 离线映射、校验与 --help 不需要网络依赖
    requests = None

REQUEST_EXCEPTION = requests.RequestException if requests else OSError


CSV_FIELDS = [
    "product_id", "item_number", "url", "title", "brand",
    "color", "capacity", "material", "selling_price", "currency",
    "main_image", "images", "description", "features", "specs", "reviews",
    "product_group_id", "variation_theme", "variation_value", "scraped_at",
    "status", "error", "category_path", "category_level_1",
    "category_level_2", "category_level_3", "category_leaf",
    "category_node_ids", "category_source", "category_complete",
]

CB_API = "https://api.cloudbypass.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
STATE_DIR = "state"
SITEMAP_CACHE_DIR = os.path.join(STATE_DIR, "sitemaps")
PROCESSED_FILE = os.path.join(STATE_DIR, "processed_sku_csv.txt")
SKIPPED_FILE = os.path.join(STATE_DIR, "skipped_sku_csv.jsonl")
RETRY_FILE = os.path.join(STATE_DIR, "retry_sku_csv.jsonl")
N_SITEMAPS = 301
DEFAULT_OUTPUT = os.path.join("out", "TRENDYOL_SKU_DATA.csv")
DEFAULT_EXCLUDED_CATEGORY_TERMS = ("mücevher", "takı", "jewelry", "jewellery")
DEFAULT_MIN_SELLING_PRICE = Decimal("5000")

_credit_lock = threading.Lock()
_credits_used = {"page": 0}
_part_lock = threading.Lock()
_thread_local = threading.local()
_part_counter = [500]
_PART_MIN, _PART_MAX = 500, 990
_PART_FILE = os.path.join(STATE_DIR, "part_counter.txt")


def env(name, default=""):
    return os.environ.get(name, default).strip()


def require_runtime_config():
    """Fail fast without printing any secret value."""
    if requests is None:
        raise SystemExit("缺少 Python 依赖 requests；请先运行 bash deploy_run.sh setup")
    required = ("CB_APIKEY", "PROXY_USER", "PROXY_PASS", "PROXY_HOST")
    missing = [name for name in required if not env(name)]
    if missing:
        raise SystemExit(
            "缺少运行环境变量: " + ", ".join(missing)
            + "。请参考 trendyol.env.example。"
        )


def _load_part_counter():
    try:
        value = int(open(_PART_FILE, encoding="utf-8").read().strip())
        if _PART_MIN <= value < _PART_MAX:
            return value
    except (OSError, ValueError):
        pass
    return _PART_MIN


def _new_session():
    with _part_lock:
        part = _part_counter[0]
        next_part = part + 1
        if next_part >= _PART_MAX:
            next_part = _PART_MIN
        _part_counter[0] = next_part
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            with open(_PART_FILE, "w", encoding="utf-8") as handle:
                handle.write(str(next_part))
        except OSError:
            pass
    return {"part": part, "sid": f"tyol{part}"}


def get_session():
    if not getattr(_thread_local, "session", None):
        _thread_local.session = _new_session()
    return _thread_local.session


def rotate_session():
    _thread_local.session = _new_session()
    return _thread_local.session


def _new_http_session():
    if requests is None:
        raise RuntimeError("requests is not installed")
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=2,
        pool_maxsize=4,
        max_retries=0,
        pool_block=True,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_control_http_session():
    """Thread-owned pool for CloudBypass and public control-plane requests."""
    session = getattr(_thread_local, "control_http_session", None)
    if session is None:
        session = _new_http_session()
        _thread_local.control_http_session = session
    return session


def get_direct_http_session():
    """Thread-owned pool pinned to one Turkish proxy identity for its lifetime."""
    session = getattr(_thread_local, "direct_http_session", None)
    if session is None:
        session = _new_http_session()
        proxy_url = proxy_http(get_session()["sid"])
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        _thread_local.direct_http_session = session
    return session


def get_http_session():
    """Backward-compatible alias for the direct product-request Session."""
    return get_direct_http_session()


def close_http_session():
    for name in ("direct_http_session", "control_http_session"):
        session = getattr(_thread_local, name, None)
        if session is not None:
            session.close()
            setattr(_thread_local, name, None)


def proxy_http(sid):
    user_template = env("PROXY_USER")
    user = quote(user_template.format(sid=sid), safe="")
    password = quote(env("PROXY_PASS"), safe="")
    return f"http://{user}:{password}@{env('PROXY_HOST')}"


def balance():
    key, email = env("CB_APIKEY"), env("CB_EMAIL")
    if not key or not email:
        return None
    try:
        response = get_control_http_session().get(
            "https://console.cloudbypass.com/api/v1/balance",
            params={"apikey": key, "email": email},
            timeout=20,
        )
        response.raise_for_status()
        return response.json().get("balance")
    except (REQUEST_EXCEPTION, ValueError):
        return None


def cb_get(path, host, version, extra=None, tries=4, cost_key=None):
    for attempt in range(tries):
        proxy_session = get_session()
        headers = {
            "x-cb-apikey": env("CB_APIKEY"),
            "x-cb-host": host,
            "x-cb-protocol": "https",
            "user-agent": UA,
            "accept": "text/html,application/json,*/*",
            "accept-language": "tr-TR,tr;q=0.9",
        }
        if extra:
            headers.update(extra)
        if version:
            headers.update({
                "x-cb-version": str(version),
                "x-cb-part": str(proxy_session["part"]),
                "x-cb-proxy": proxy_http(proxy_session["sid"]),
            })
        try:
            response = get_control_http_session().get(CB_API + path, headers=headers, timeout=35)
            cb_status = response.headers.get("x-cb-status")
            blocked = "Attention Required" in response.text[:400]
            if response.status_code == 200 and cb_status in ("ok", None) and not blocked:
                if cost_key:
                    with _credit_lock:
                        _credits_used[cost_key] += 3 if version == 2 else 1
                return response.status_code, response.text
        except REQUEST_EXCEPTION:
            pass
        if version:
            rotate_session()
        time.sleep(0.5 * (attempt + 1))
    return None, None


def fetch_sitemap(number, tries=4):
    """Return [(path, product_id), ...] and cache the public sitemap locally."""
    os.makedirs(SITEMAP_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(SITEMAP_CACHE_DIR, f"products{number}.txt")
    if os.path.exists(cache_path):
        items = []
        with open(cache_path, encoding="utf-8") as handle:
            for line in handle:
                if "\t" in line:
                    path, product_id = line.rstrip("\n").split("\t", 1)
                    items.append((path, product_id))
        return items

    url = f"https://www.trendyol.com/sitemap_products{number}.xml"
    for attempt in range(tries):
        try:
            response = get_control_http_session().get(
                url,
                headers={"user-agent": UA, "accept": "application/xml,*/*"},
                timeout=120,
            )
            if response.status_code == 200 and "<loc>" in response.text:
                items = []
                for product_url in re.findall(r"<loc>([^<]+)</loc>", response.text):
                    match = re.search(r"trendyol\.com(/[^\s<]*?-p-(\d{4,}))", product_url)
                    if match:
                        items.append((match.group(1), match.group(2)))
                with open(cache_path, "w", encoding="utf-8", newline="") as handle:
                    handle.write("\n".join(f"{path}\t{pid}" for path, pid in items))
                return items
        except REQUEST_EXCEPTION:
            pass
        time.sleep(2 * (attempt + 1))
    return []


def _match_object(source, start):
    depth, in_string, escaped = 0, False, False
    for index in range(start, len(source)):
        char = source[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        else:
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return source[start:index + 1]
    return None


def extract_state(page_html):
    anchor = page_html.find('"categoryTree"')
    if anchor < 0:
        return None
    best = None
    for index in range(anchor, max(0, anchor - 12000), -1):
        if page_html[index] != "{":
            continue
        candidate = _match_object(page_html, index)
        if not candidate:
            continue
        if all(marker in candidate for marker in ('"images"', '"categoryTree"', '"merchantListing"')):
            try:
                best = json.loads(candidate)
            except ValueError:
                continue
    return best


_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def clean_text(value):
    if value is None:
        return ""
    text = html_lib.unescape(str(value))
    text = _TAG_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def clean_list_item(value):
    return clean_text(value).replace("|", " ")


def unique_nonempty(values):
    result, seen = [], set()
    for value in values:
        cleaned = clean_text(value)
        if cleaned and cleaned not in seen:
            result.append(cleaned)
            seen.add(cleaned)
    return result


def normalize_url(value):
    value = clean_text(value)
    if not value:
        return ""
    return value if value.startswith("http") else "https:" + value


def _first_value(mapping, keys):
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", []):
            return value
    return None


def _review_images(value):
    result = []
    if not isinstance(value, list):
        return result
    for item in value:
        if isinstance(item, str):
            url = normalize_url(item)
        elif isinstance(item, dict):
            url = normalize_url(_first_value(item, ("url", "path", "fullSizeUrl", "imageUrl")))
        else:
            url = ""
        if url and url not in result:
            result.append(url)
    return result


def _normalize_review(candidate):
    """Recognize Trendyol-like embedded review objects without guessing plain text blocks."""
    if not isinstance(candidate, dict):
        return None
    body = clean_text(_first_value(candidate, ("comment", "commentText", "reviewText", "review")))
    title = clean_text(candidate.get("title"))
    stars = _first_value(candidate, ("rate", "rating", "stars", "score"))
    if not (body or title) or stars is None:
        return None
    try:
        stars = float(stars)
    except (TypeError, ValueError):
        return None
    if not 1 <= stars <= 5:
        return None
    if stars.is_integer():
        stars = int(stars)
    images = _review_images(
        _first_value(candidate, ("mediaFiles", "images", "photos", "imageUrls")) or []
    )
    return {
        "stars": stars,
        "title": title,
        "body": body,
        "date": clean_text(
            _first_value(candidate, ("commentDateISOtype", "commentDate", "createdDate", "date"))
        ),
        "author": clean_text(
            _first_value(candidate, ("userFullName", "userName", "author", "customerName"))
        ),
        "images": images,
    }


def _collect_reviews(value, output, seen, limit):
    if len(output) >= limit:
        return
    if isinstance(value, dict):
        review = _normalize_review(value)
        if review:
            key = (review["body"], review["title"], review["date"], review["author"])
            if key not in seen:
                seen.add(key)
                output.append(review)
                if len(output) >= limit:
                    return
        for child in value.values():
            _collect_reviews(child, output, seen, limit)
            if len(output) >= limit:
                return
    elif isinstance(value, list):
        for child in value:
            _collect_reviews(child, output, seen, limit)
            if len(output) >= limit:
                return


def extract_page_reviews(state, page_html="", limit=10):
    """Extract only reviews already embedded in the product detail response."""
    output, seen = [], set()
    _collect_reviews(state, output, seen, limit)
    if len(output) >= limit or not page_html:
        return output

    # Some page versions keep reviews in a sibling JSON object rather than product state.
    markers = ('"userFullName"', '"commentDateISOtype"', '"commentText"', '"reviewText"')
    for marker in markers:
        position = page_html.find(marker)
        while position >= 0 and len(output) < limit:
            lower = max(0, position - 5000)
            start = page_html.rfind("{", lower, position)
            attempts = 0
            while start >= lower and attempts < 20:
                candidate_text = _match_object(page_html, start)
                if candidate_text and start + len(candidate_text) > position:
                    try:
                        candidate = json.loads(candidate_text)
                    except ValueError:
                        candidate = None
                    if candidate is not None:
                        _collect_reviews(candidate, output, seen, limit)
                        break
                start = page_html.rfind("{", lower, start)
                attempts += 1
            position = page_html.find(marker, position + len(marker))
    return output


def format_price(value):
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return None


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _product(state):
    return state.get("product", state) if isinstance(state, dict) else {}


_TURKISH_TRANSLATION = str.maketrans("ışğüöçİŞĞÜÖÇ", "isguociSGUOC")
_DIMENSION_ALIASES = {
    "color": ("dsmcolor", "color", "colour", "renk"),
    "capacity": (
        "internalmemory", "storagecapacity", "storagesize", "storage",
        "dahilihafiza", "hafizakapasitesi", "memorycapacity", "capacity",
        "kapasite", "hacim", "volume",
    ),
    "material": ("material", "materyal", "malzeme", "fabric", "kumas"),
}


def _normalize_attribute_name(value):
    value = clean_text(value).translate(_TURKISH_TRANSLATION).casefold()
    return re.sub(r"[^a-z0-9]+", "", value)


def _dimension_values(product):
    """Return fixed color/capacity/material fields; missing page values stay empty."""
    sources = []
    for name, value in (product.get("slicingAttributes") or {}).items():
        sources.append((name, value))
    for attribute in product.get("attributes") or []:
        value = clean_text((attribute.get("value") or {}).get("name"))
        sources.append(((attribute.get("key") or {}).get("name"), value))
        sources.append((attribute.get("type"), value))

    result = {"color": "", "capacity": "", "material": ""}
    for field, aliases in _DIMENSION_ALIASES.items():
        for name, value in sources:
            normalized = _normalize_attribute_name(name)
            if field == "capacity" and "ram" in normalized:
                continue
            if any(alias == normalized or alias in normalized for alias in aliases):
                cleaned = clean_text(value)
                if cleaned:
                    result[field] = cleaned
                    break
    return result


def _variant_price(variant, winner):
    price = variant.get("price") or {}
    if isinstance(price, dict):
        selling = price.get("sellingPrice") or {}
        if isinstance(selling, dict) and selling.get("value") is not None:
            return selling.get("value"), clean_text(price.get("currency"))
        if price.get("value") is not None:
            return price.get("value"), clean_text(price.get("currency"))
    winner_price = winner.get("price") or {}
    winner_selling = winner_price.get("sellingPrice") or {}
    return winner_selling.get("value"), clean_text(winner_price.get("currency"))


def _product_variants(product):
    """Merge every detail-response SKU variant by its native itemNumber."""
    listing = product.get("merchantListing") or {}
    winner = listing.get("winnerVariant") or {}
    merged = {}
    order = []
    for source in (product.get("variants") or [], listing.get("variants") or [], [winner]):
        for variant in source:
            if not isinstance(variant, dict):
                continue
            item_number = clean_text(variant.get("itemNumber"))
            if not item_number:
                continue
            if item_number not in merged:
                merged[item_number] = {}
                order.append(item_number)
            merged[item_number].update(variant)
    if not merged:
        fallback = dict(winner)
        fallback["itemNumber"] = clean_text(product.get("id"))
        merged[fallback["itemNumber"]] = fallback
        order.append(fallback["itemNumber"])
    variants = []
    for item_number in order:
        variant = merged[item_number]
        variant["itemNumber"] = item_number
        variants.append(variant)
    return variants or [dict(winner, itemNumber=clean_text(product.get("id")))]


def _variation_pairs(product, variant, variant_count):
    slicing = variant.get("slicingAttributes") or product.get("slicingAttributes") or {}
    pairs = [
        (clean_list_item(name), clean_list_item(value))
        for name, value in slicing.items()
        if clean_list_item(name) and clean_list_item(value)
    ]
    variant_value = clean_list_item(variant.get("beautifiedValue") or variant.get("value"))
    existing_values = {value.casefold() for _, value in pairs}
    if variant_value and variant_value.casefold() not in existing_values:
        theme = clean_list_item(
            product.get("variantAttributeName") or product.get("variantName") or "Variant"
        )
        normalized_theme = _normalize_attribute_name(theme)
        replaced = False
        for index, (name, _) in enumerate(pairs):
            if _normalize_attribute_name(name) == normalized_theme:
                pairs[index] = (name, variant_value)
                replaced = True
                break
        if not replaced:
            pairs.append((theme, variant_value))
    elif variant_count > 1 and not variant_value:
        pairs.append(("itemNumber", clean_list_item(variant.get("itemNumber"))))
    return pairs


def build_record(state, product_path=None, scraped_at=None, reviews=None, variant=None,
                 variant_count=1):
    """Map one Trendyol variant into one fixed 30-column SKU row."""
    product = _product(state)
    listing = product.get("merchantListing") or {}
    winner = listing.get("winnerVariant") or {}
    variant = variant or winner or {"itemNumber": product.get("id")}

    product_id = clean_text(product.get("id"))
    product_url = (
        "https://www.trendyol.com" + product_path
        if product_path and product_path.startswith("/")
        else f"https://www.trendyol.com/-p-{product_id}"
    )
    brand = clean_text((product.get("brand") or {}).get("name"))
    image_source = variant.get("images") or product.get("images") or []
    images = unique_nonempty(normalize_url(item) for item in image_source)

    specs = {}
    feature_items = []
    for attribute in product.get("attributes") or []:
        name = clean_text((attribute.get("key") or {}).get("name"))
        value = clean_text((attribute.get("value") or {}).get("name"))
        if not name or not value:
            continue
        specs[name] = value
        feature_items.append(f"{clean_list_item(name)}: {clean_list_item(value)}")

    tree = product.get("categoryTree") or []
    category_nodes = [
        (clean_list_item(node.get("name")), clean_list_item(node.get("id")))
        for node in tree
        if clean_list_item(node.get("name"))
    ]
    category_names = [name for name, _ in category_nodes]
    # node ID 只有在能与完整路径一一对应时才交付，否则按可选字段留空。
    category_ids = [node_id for _, node_id in category_nodes]
    if not all(category_ids):
        category_ids = []

    variation_pairs = _variation_pairs(product, variant, variant_count)
    group_id = clean_text(product.get("productGroupId"))
    if group_id and variation_pairs:
        product_group_id = group_id
        variation_theme = "|".join(pair[0] for pair in variation_pairs)
        variation_value = "|".join(pair[1] for pair in variation_pairs)
    else:
        product_group_id = product_id
        variation_theme = ""
        variation_value = ""

    item_number = clean_text(variant.get("itemNumber") or product_id)
    raw_price, currency = _variant_price(variant, winner)
    selling_price = format_price(raw_price)
    dimensions = _dimension_values(product)
    normalized_themes = [_normalize_attribute_name(name) for name, _ in variation_pairs]
    for (name, value), normalized in zip(variation_pairs, normalized_themes):
        if any(alias == normalized or alias in normalized for alias in _DIMENSION_ALIASES["color"]):
            dimensions["color"] = value
        elif "ram" not in normalized and any(
            alias == normalized or alias in normalized for alias in _DIMENSION_ALIASES["capacity"]
        ):
            dimensions["capacity"] = value
        elif any(
            alias == normalized or alias in normalized for alias in _DIMENSION_ALIASES["material"]
        ):
            dimensions["material"] = value
    return {
        "product_id": product_id,
        "item_number": item_number,
        "url": product_url,
        "title": clean_text(product.get("name")),
        "brand": brand,
        "color": dimensions["color"],
        "capacity": dimensions["capacity"],
        "material": dimensions["material"],
        "selling_price": f"{selling_price:.2f}" if selling_price is not None else "",
        "currency": currency or "TRY",
        "main_image": images[0] if images else "",
        "images": "|".join(clean_list_item(item) for item in images),
        "description": "",
        "features": "|".join(unique_nonempty(feature_items)),
        "specs": json.dumps(specs, ensure_ascii=False, separators=(",", ":")),
        "reviews": json.dumps((reviews or [])[:10], ensure_ascii=False, separators=(",", ":")),
        "product_group_id": product_group_id,
        "variation_theme": variation_theme,
        "variation_value": variation_value,
        "scraped_at": scraped_at or utc_now(),
        "status": "ok",
        "error": "",
        "category_path": "|".join(category_names),
        "category_level_1": category_names[0] if category_names else "",
        "category_level_2": category_names[1] if len(category_names) >= 2 else "",
        "category_level_3": category_names[2] if len(category_names) >= 3 else "",
        "category_leaf": category_names[-1] if category_names else "",
        "category_node_ids": "|".join(category_ids),
        "category_source": "trendyol_category_tree",
        "category_complete": "true" if category_names else "false",
    }


def build_records(state, product_path=None, scraped_at=None, reviews=None):
    """Expand all SKU variants already present in one product detail response."""
    product = _product(state)
    variants = _product_variants(product)
    timestamp = scraped_at or utc_now()
    records, seen_items = [], set()
    for variant in variants:
        record = build_record(
            state,
            product_path=product_path,
            scraped_at=timestamp,
            reviews=reviews,
            variant=variant,
            variant_count=len(variants),
        )
        if record["item_number"] and record["item_number"] not in seen_items:
            seen_items.add(record["item_number"])
            records.append(record)
    return records


def is_excluded_category(record, terms):
    path = record.get("category_path", "").casefold()
    return any(term.casefold() in path for term in terms if term)


def validate_record(record, min_selling_price=DEFAULT_MIN_SELLING_PRICE):
    errors = []
    missing = [
        field for field in (
            "product_id", "item_number", "url", "title", "brand", "main_image", "category_path"
        )
        if not record.get(field)
    ]
    if missing:
        errors.append("missing_required:" + ",".join(missing))
    try:
        if Decimal(record.get("selling_price", "")) <= Decimal(str(min_selling_price)):
            errors.append("selling_price_not_above_minimum")
    except (InvalidOperation, TypeError):
        errors.append("invalid_price")
    if record.get("currency") != "TRY":
        errors.append("invalid_currency")
    try:
        if not isinstance(json.loads(record.get("specs", "")), dict):
            errors.append("specs_not_object")
    except (TypeError, ValueError):
        errors.append("invalid_specs_json")
    try:
        reviews = json.loads(record.get("reviews", ""))
        if not isinstance(reviews, list):
            errors.append("reviews_not_array")
        elif len(reviews) > 10:
            errors.append("too_many_reviews")
        else:
            for review in reviews:
                if not isinstance(review, dict):
                    errors.append("invalid_review_object")
                    break
                try:
                    stars = float(review.get("stars"))
                except (TypeError, ValueError):
                    errors.append("invalid_review_stars")
                    break
                if not 1 <= stars <= 5:
                    errors.append("invalid_review_stars")
                    break
                if not (clean_text(review.get("title")) or clean_text(review.get("body"))):
                    errors.append("empty_review_content")
                    break
    except (TypeError, ValueError):
        errors.append("invalid_reviews_json")
    theme, value = record.get("variation_theme", ""), record.get("variation_value", "")
    if bool(theme) != bool(value):
        errors.append("variation_pair_incomplete")
    elif theme and len(theme.split("|")) != len(value.split("|")):
        errors.append("variation_dimension_mismatch")
    elif not theme and record.get("product_group_id") != record.get("product_id"):
        errors.append("standalone_parent_mismatch")
    categories = record.get("category_path", "").split("|") if record.get("category_path") else []
    if categories:
        if record.get("category_level_1") != categories[0]:
            errors.append("category_level_1_mismatch")
        if record.get("category_leaf") != categories[-1]:
            errors.append("category_leaf_mismatch")
    if record.get("category_source") != "trendyol_category_tree":
        errors.append("invalid_category_source")
    if record.get("category_complete") != "true":
        errors.append("incomplete_category")
    if record.get("status") != "ok" or record.get("error"):
        errors.append("invalid_success_status")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", record.get("scraped_at", "")):
        errors.append("invalid_scraped_at")
    return errors


class CsvDataset:
    """Thread-safe append-only CSV writer with platform-specific resume state."""

    def __init__(self, output_path, total, category_cap, min_selling_price=DEFAULT_MIN_SELLING_PRICE):
        self.output_path = output_path
        self.total = total
        self.category_cap = category_cap
        self.min_selling_price = Decimal(str(min_selling_price))
        self.lock = threading.Lock()
        self.seen = set()
        self.done = set()
        self.category_counts = Counter()
        self.kept = 0
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        os.makedirs(STATE_DIR, exist_ok=True)
        self._load_existing()
        self._processed_handle = open(PROCESSED_FILE, "a", encoding="utf-8", newline="")
        self._skipped_handle = open(SKIPPED_FILE, "a", encoding="utf-8", newline="")
        self._retry_handle = open(RETRY_FILE, "a", encoding="utf-8", newline="")
        is_new = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
        self._csv_handle = open(output_path, "a", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._csv_handle, fieldnames=CSV_FIELDS, extrasaction="raise")
        if is_new:
            self._writer.writeheader()
            self._csv_handle.flush()
        self.start_kept = self.kept

    @staticmethod
    def _category_key(record):
        node_ids = record.get("category_node_ids", "")
        return node_ids.split("|")[-1] if node_ids else record.get("category_path", "")

    def _load_existing(self):
        if os.path.exists(self.output_path) and os.path.getsize(self.output_path) > 0:
            with open(self.output_path, encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != CSV_FIELDS:
                    raise RuntimeError("现有 CSV 表头与 30 列 SKU 交付规范不一致，拒绝继续追加")
                for record in reader:
                    item_number = record.get("item_number", "")
                    if not item_number or item_number in self.seen:
                        continue
                    self.seen.add(item_number)
                    self.category_counts[self._category_key(record)] += 1
                    self.kept += 1
        if os.path.exists(PROCESSED_FILE):
            with open(PROCESSED_FILE, encoding="utf-8") as handle:
                self.done.update(line.strip() for line in handle if line.strip())

    def full(self):
        with self.lock:
            return self.kept >= self.total

    def wants(self, record):
        with self.lock:
            category = self._category_key(record)
            return (
                record["item_number"] not in self.seen
                and self.kept < self.total
                and (self.category_cap <= 0 or self.category_counts[category] < self.category_cap)
            )

    def _mark_done_locked(self, product_id):
        product_id = str(product_id)
        if product_id not in self.done:
            self.done.add(product_id)
            self._processed_handle.write(product_id + "\n")
            self._processed_handle.flush()

    def add(self, record):
        errors = validate_record(record, self.min_selling_price)
        if errors:
            raise ValueError(";".join(errors))
        with self.lock:
            category = self._category_key(record)
            if record["item_number"] in self.seen or self.kept >= self.total:
                return False
            if self.category_cap > 0 and self.category_counts[category] >= self.category_cap:
                return False
            self._writer.writerow({field: record.get(field, "") for field in CSV_FIELDS})
            self._csv_handle.flush()
            self.seen.add(record["item_number"])
            self.category_counts[category] += 1
            self.kept += 1
            return True

    def mark_done(self, product_id):
        with self.lock:
            self._mark_done_locked(product_id)

    def mark_skipped(self, product_id, reason):
        with self.lock:
            payload = {"product_id": str(product_id), "reason": reason, "at": utc_now()}
            self._skipped_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            self._skipped_handle.flush()
            self._mark_done_locked(product_id)

    def mark_retry(self, product_id, reason):
        with self.lock:
            payload = {"product_id": str(product_id), "reason": reason, "at": utc_now()}
            self._retry_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            self._retry_handle.flush()

    def close(self):
        for handle in (
            self._csv_handle, self._processed_handle, self._skipped_handle, self._retry_handle
        ):
            handle.close()


def fetch_product(path, product_id, dataset, excluded_terms):
    _, page_html = cb_get(path, "www.trendyol.com", 2, cost_key="page")
    if not page_html:
        return "retry", None, "product_page_failed"
    state = extract_state(page_html)
    if not state:
        return "retry", None, "product_state_not_found"

    page_reviews = extract_page_reviews(state, page_html, limit=10)
    records = build_records(state, product_path=path, reviews=page_reviews)
    if not records:
        return "skip", None, "no_sku_variants"
    if is_excluded_category(records[0], excluded_terms):
        return "skip", None, "excluded_jewelry_category"

    eligible, reasons = [], []
    for record in records:
        validation_errors = validate_record(record, dataset.min_selling_price)
        if validation_errors:
            reasons.extend(validation_errors)
            continue
        if dataset.wants(record):
            eligible.append(record)
    if not eligible:
        reason = ";".join(sorted(set(reasons))) if reasons else "duplicate_or_category_cap"
        return "skip", None, reason
    return "ok", eligible, ""


def main():
    parser = argparse.ArgumentParser(description="Trendyol -> 30列SKU级标准CSV采集器")
    parser.add_argument("--total", type=int, default=300000, help="成功交付 SKU 总行数")
    parser.add_argument("--cap", type=int, default=90, help="每个叶子分类最多保留SKU行数；0为不限")
    parser.add_argument("--workers", type=int, default=25)
    parser.add_argument("--sitemaps", type=int, default=N_SITEMAPS)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--min-credits", type=int, default=15000)
    parser.add_argument(
        "--min-selling-price",
        type=Decimal,
        default=Decimal(env("MIN_SELLING_PRICE", "5000")),
        help="仅保留 selling_price 严格大于该 TRY 金额的商品",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--exclude-category-terms",
        default=env("EXCLUDED_CATEGORY_TERMS", "|".join(DEFAULT_EXCLUDED_CATEGORY_TERMS)),
        help="用 | 分隔的排除分类关键词",
    )
    args = parser.parse_args()

    if args.total <= 0 or args.workers <= 0 or args.sitemaps <= 0:
        parser.error("total/workers/sitemaps 必须大于 0")
    require_runtime_config()
    _part_counter[0] = _load_part_counter()
    excluded_terms = [term.strip() for term in args.exclude_category_terms.split("|") if term.strip()]

    dataset = CsvDataset(args.output, args.total, args.cap, args.min_selling_price)
    starting_balance = balance()
    print(
        f"目标:{args.total} 每类封顶:{args.cap} 并发:{args.workers} "
        f"价格口径:Trendyol 当前 TRY 实价 > {args.min_selling_price}"
    )
    print(f"输出:{args.output} | 起始余额:{starting_balance} | 断点已有:{dataset.kept}")

    work_queue = queue.Queue(maxsize=args.workers * 4)
    stop = threading.Event()
    started_at = time.time()
    processed = 0
    last_progress = 0.0

    def credit_guard_loop():
        if args.min_credits <= 0:
            return
        while not stop.wait(90):
            current = balance()
            if current is not None and current < args.min_credits:
                print(f"[余额告警] {current} < {args.min_credits}，停止新请求")
                stop.set()
                return

    def credit_guard():
        try:
            credit_guard_loop()
        finally:
            close_http_session()

    def worker_loop():
        while True:
            try:
                item = work_queue.get(timeout=2)
            except queue.Empty:
                if stop.is_set():
                    return
                continue
            try:
                if item is None:
                    return
                path, product_id = item
                if stop.is_set() or product_id in dataset.done:
                    continue
                outcome, records, reason = fetch_product(path, product_id, dataset, excluded_terms)
                if outcome == "ok":
                    for record in records:
                        dataset.add(record)
                    dataset.mark_done(product_id)
                elif outcome == "skip":
                    dataset.mark_skipped(product_id, reason)
                else:
                    dataset.mark_retry(product_id, reason)
                if dataset.full():
                    stop.set()
            except Exception as exc:  # keep other workers alive, retain product for a later run
                if item is not None:
                    dataset.mark_retry(item[1], f"worker_error:{type(exc).__name__}")
            finally:
                work_queue.task_done()

    def worker():
        try:
            worker_loop()
        finally:
            close_http_session()

    threading.Thread(target=credit_guard, daemon=True).start()
    workers = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    for thread in workers:
        thread.start()

    try:
        for number in range(1, args.sitemaps + 1):
            if stop.is_set() or dataset.full():
                break
            items = fetch_sitemap(number)
            if args.shuffle:
                random.shuffle(items)
            print(
                f"[sitemap {number}] {len(items)} URL | 已采{dataset.kept} "
                f"分类{len(dataset.category_counts)} credits~{sum(_credits_used.values())}"
            )
            for path, product_id in items:
                if stop.is_set() or dataset.full():
                    break
                if product_id in dataset.done:
                    continue
                work_queue.put((path, product_id))
                processed += 1
                now = time.time()
                if now - last_progress >= 30:
                    last_progress = now
                    elapsed = max(now - started_at, 1)
                    new_rows = dataset.kept - dataset.start_kept
                    print(
                        f"进度: 已采{dataset.kept}(+{new_rows}) 处理{processed} "
                        f"{elapsed:.0f}s ~{new_rows / elapsed * 3600:.0f}条/h "
                        f"credits~{sum(_credits_used.values())}"
                    )
        work_queue.join()
    except KeyboardInterrupt:
        print("收到中断，等待已开始的请求安全落盘……")
        stop.set()
        work_queue.join()
    finally:
        stop.set()
        for _ in workers:
            work_queue.put(None)
        for thread in workers:
            thread.join(timeout=5)
        ending_balance = balance()
        close_http_session()
        elapsed = time.time() - started_at
        dataset.close()
        print("=" * 70)
        print(
            f"结束: CSV共{dataset.kept}个SKU / {len(dataset.category_counts)}个叶子分类 | "
            f"用时{elapsed:.0f}s | 余额 {starting_balance}->{ending_balance}"
        )


if __name__ == "__main__":
    # Keep the historical filename safe: direct execution now enters the
    # SQLite WAL pipeline instead of the legacy CSV checkpoint runner above.
    from pipeline import main as pipeline_main
    pipeline_main()
