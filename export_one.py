# -*- coding: utf-8 -*-
"""把本地 Trendyol 状态样本展开为 30 列 SKU CSV，用于离线验收。"""
import argparse
import csv
import json

from trendyol_scraper import CSV_FIELDS, build_records, validate_record


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("state", help="详情页内嵌状态 JSON")
    parser.add_argument("--path", help="真实商品 URL path")
    parser.add_argument("--output", default="out/TRENDYOL_SKU_DATA_SAMPLE.csv")
    args = parser.parse_args()

    state = load_json(args.state)
    records = build_records(state, args.path)
    for record in records:
        errors = validate_record(record)
        if errors:
            raise SystemExit(
                f"SKU {record.get('item_number')} 不符合交付规范: " + "; ".join(errors)
            )

    import os
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"已导出: {args.output}")


if __name__ == "__main__":
    main()
