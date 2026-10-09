"""Đẩy URL trang danh sách vào Redis cho spider.

Cách dùng (chạy tại thư mục gốc dự án):
    python scripts/seed_urls.py chotot                      # tất cả nhóm
    python scripts/seed_urls.py chotot dodientu             # một nhóm
    python scripts/seed_urls.py chotot dodientu --pages 2   # giới hạn số trang để test
    python scripts/seed_urls.py chotot --dry-run            # chỉ in ra, không đẩy vào Redis
    python scripts/seed_urls.py chotot --clear              # xóa hàng đợi cũ trước khi đẩy
"""

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import redis 

from hb_1014.utils.config_loader import ConfigError, load_categories 
from hb_1014.utils.redis_client import get_redis 

META_FIELDS = ("source", "group", "vertical", "category", "category_name", "max_items_per_page")


def build_page_url(cat, page):
    sep = "&" if "?" in cat["url"] else "?"
    return f"{cat['url']}{sep}{urlencode({cat['page_param']: page})}"


def build_payloads(categories, page_limit=None):
    for cat in categories:
        total = cat["max_pages"]
        if page_limit:
            total = min(page_limit, total)

        meta = {k: cat[k] for k in META_FIELDS}
        start = cat["start_page"]

        for page in range(start, start + total):
            yield {
                "url": build_page_url(cat, page),
                "meta": {**meta, "page": page},
            }

def main():
    parser = argparse.ArgumentParser(description="Đẩy URL vào Redis cho spider")
    parser.add_argument("spider", help="tên spider, trùng tên thư mục trong config/")
    parser.add_argument("groups", nargs="*", help="tên nhóm, bỏ trống = tất cả")
    parser.add_argument("--pages", type=int, help="giới hạn số trang mỗi danh mục")
    parser.add_argument("--clear", action="store_true", help="xóa hàng đợi cũ trước")
    parser.add_argument("--dry-run", action="store_true", help="chỉ in ra, không đẩy")
    args = parser.parse_args()

    try:
        categories = load_categories(args.spider, args.groups or ["all"])
    except ConfigError as e:
        print(f"Lỗi cấu hình: {e}")
        sys.exit(1)

    if not categories:
        print("Không có danh mục nào đang bật (enabled: true).")
        sys.exit(1)

    payloads = list(build_payloads(categories, args.pages))
    key = f"{args.spider}:start_urls"

    if args.dry_run:
        for p in payloads:
            print(p["url"])
        print(f"\n[dry-run] {len(payloads)} URL sẽ được đẩy vào key '{key}'")
        return

    r = get_redis()
    try:
        r.ping()
    except redis.ConnectionError:
        print("Không kết nối được Redis. Kiểm tra docker compose ps và file .env")
        sys.exit(1)

    if args.clear:
        r.delete(key)
        print(f"Đã xóa hàng đợi cũ '{key}'")

    pipe = r.pipeline()
    for p in payloads:
        pipe.rpush(key, json.dumps(p, ensure_ascii=False))
    pipe.execute()

    print(f"Đã đẩy {len(payloads)} URL vào '{key}'")
    for cat in categories:
        count = sum(1 for p in payloads if p["meta"]["category"] == cat["category"])
        print(f"  - [{cat['group']}] {cat['category_name']}: {count} trang")
    print(f"Tổng số URL đang chờ trong hàng đợi: {r.llen(key)}")


if __name__ == "__main__":
    main()