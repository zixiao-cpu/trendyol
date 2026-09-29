# -*- coding: utf-8 -*-
"""验证 Trendyol 30 列 SKU 交付 CSV。只使用 Python 标准库。"""
import argparse
import csv
import json
import re
from decimal import Decimal, InvalidOperation

from trendyol_scraper import CSV_FIELDS, DEFAULT_MIN_SELLING_PRICE


def validate_row(row, row_number, seen, min_selling_price=DEFAULT_MIN_SELLING_PRICE):
    issues = []
    sku = row.get("item_number", "")
    if not sku or sku in seen:
        issues.append("item_number 为空或重复")
    seen.add(sku)
    product_id = row.get("product_id", "")
    if product_id and not re.search(
        rf"-p-{re.escape(product_id)}(?:$|[/?#])", row.get("url", "")
    ):
        issues.append("url 与 product_id 不一致")
    for field in ("product_id", "item_number", "title", "brand", "main_image", "category_path"):
        if not row.get(field):
            issues.append(f"{field} 为空")
    try:
        price = Decimal(row.get("selling_price", ""))
        if price <= min_selling_price:
            issues.append(f"selling_price 必须严格大于 {min_selling_price} TRY")
        if not re.fullmatch(r"\d+\.\d{2}", row.get("selling_price", "")):
            issues.append("价格不是两位小数")
    except InvalidOperation:
        issues.append("价格无效")
    if row.get("currency") != "TRY":
        issues.append("currency 不是 TRY")
    try:
        if not isinstance(json.loads(row.get("specs", "")), dict):
            issues.append("specs 不是 JSON object")
    except ValueError:
        issues.append("specs JSON 无效")
    try:
        reviews = json.loads(row.get("reviews", ""))
        if not isinstance(reviews, list) or len(reviews) > 10:
            issues.append("reviews 不是数组或超过 10 条")
        elif isinstance(reviews, list):
            for review in reviews:
                stars = review.get("stars")
                if stars is None or not 1 <= float(stars) <= 5:
                    issues.append("评论评分超出 1-5")
                if not (str(review.get("title", "")).strip() or str(review.get("body", "")).strip()):
                    issues.append("评论标题和正文同时为空")
    except (ValueError, TypeError, AttributeError):
        issues.append("reviews JSON 无效")
    theme, value = row.get("variation_theme", ""), row.get("variation_value", "")
    if bool(theme) != bool(value):
        issues.append("变体 theme/value 未同时存在")
    elif theme and len(theme.split("|")) != len(value.split("|")):
        issues.append("变体维度不一致")
    elif not theme and row.get("product_group_id") != product_id:
        issues.append("无变体时 product_group_id != product_id")
    path = row.get("category_path", "").split("|") if row.get("category_path") else []
    levels = path[:3] + [""] * max(0, 3 - len(path))
    if path:
        if row.get("category_level_1") != levels[0]: issues.append("一级分类不一致")
        if row.get("category_level_2") != levels[1]: issues.append("二级分类不一致")
        if row.get("category_level_3") != levels[2]: issues.append("三级分类不一致")
        if row.get("category_leaf") != path[-1]: issues.append("叶子分类不一致")
    node_ids = row.get("category_node_ids", "").split("|") if row.get("category_node_ids") else []
    if node_ids and len(node_ids) != len(path):
        issues.append("分类 node ID 数量不一致")
    if row.get("category_source") != "trendyol_category_tree":
        issues.append("category_source 无效")
    if row.get("category_complete") != "true":
        issues.append("category_complete 不是 true")
    if row.get("status") != "ok" or row.get("error"):
        issues.append("成功行 status/error 无效")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", row.get("scraped_at", "")):
        issues.append("scraped_at 不是 UTC RFC3339")
    return [f"第 {row_number} 行: {issue}" for issue in issues]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path")
    parser.add_argument("--min-selling-price", type=Decimal, default=DEFAULT_MIN_SELLING_PRICE)
    args = parser.parse_args()
    seen, issues, rows = set(), [], 0
    with open(args.csv_path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_FIELDS:
            raise SystemExit("表头不是固定 30 列或顺序不一致")
        for row_number, row in enumerate(reader, start=2):
            rows += 1
            issues.extend(validate_row(row, row_number, seen, args.min_selling_price))
            if len(issues) >= 100:
                break
    if issues:
        print("\n".join(issues[:100]))
        raise SystemExit(f"验证失败：至少 {len(issues)} 个问题")
    print(f"验证通过：{rows} 行，30 列，item_number 唯一")


if __name__ == "__main__":
    main()
