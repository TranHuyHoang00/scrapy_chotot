"""Đẩy profile giả vào Kafka topic input để TEST LOCAL.

Trên hệ thống thật, profile do Hydra scheduler đẩy vào topic
hydra-profile-scheduler-chotot; không cần chạy script này.

Mỗi danh mục bật trong config/chotot/*.yaml tạo ra 1 profile:
    {"profile_id", "url", "seed_url", "name", "type", "channel_name",
     "tenants", "vertical", "category", "max_pages", "max_items_per_page"}

Cách dùng (chạy tại thư mục gốc dự án):
    python scripts/seed_profiles.py chotot                    # tất cả nhóm
    python scripts/seed_profiles.py chotot dodientu --pages 2 # một nhóm, 2 trang/danh mục
    python scripts/seed_profiles.py chotot --dry-run          # chỉ in ra
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "chotot.settings")

from scrapy.utils.project import get_project_settings  # noqa: E402

from chotot.utils.config_loader import ConfigError, load_categories  # noqa: E402
from chotot.utils.kafka_producer import close_producer, make_producer  # noqa: E402

TEST_TENANT = {"id": "local-test", "name": "Local test", "project": "local", "category_ids": []}


def build_profile(cat, page_limit=None):
    max_pages = min(page_limit, cat["max_pages"]) if page_limit else cat["max_pages"]
    profile_id = hashlib.md5(cat["url"].encode("utf-8")).hexdigest()
    return {
        "profile_id": profile_id,
        "id": profile_id,
        "url": cat["url"],
        "seed_url": cat["url"],
        "name": cat["source"],
        "type": "page",
        "channel_name": cat["source"],
        "tenants": [TEST_TENANT],
        "vertical": cat["vertical"],
        "category": cat["category"],
        "max_pages": max_pages,
        "max_items_per_page": cat["max_items_per_page"],
    }


def main():
    parser = argparse.ArgumentParser(description="Đẩy profile test vào Kafka topic input")
    parser.add_argument("spider", help="tên spider, trùng tên thư mục trong config/")
    parser.add_argument("groups", nargs="*", help="tên nhóm, bỏ trống = tất cả")
    parser.add_argument("--pages", type=int, help="giới hạn số trang mỗi danh mục")
    parser.add_argument("--dry-run", action="store_true", help="chỉ in ra, không đẩy")
    args = parser.parse_args()

    try:
        categories = load_categories(args.spider, args.groups or ["all"])
    except ConfigError as e:
        print(f"Lỗi cấu hình: {e}")
        sys.exit(1)

    profiles = [build_profile(cat, args.pages) for cat in categories]
    if not profiles:
        print("Không có danh mục nào đang bật (enabled: true).")
        sys.exit(1)

    settings = get_project_settings()
    topic = settings.get("KAFKA_TOPIC_INPUT")

    if args.dry_run:
        for p in profiles:
            print(json.dumps(p, ensure_ascii=False))
        print(f"\n[dry-run] {len(profiles)} profile sẽ được đẩy vào topic '{topic}'")
        return

    producer = make_producer(settings)
    for p in profiles:
        producer.send(topic, value=p, key=p["profile_id"])
    close_producer(producer)

    print(f"Đã đẩy {len(profiles)} profile vào topic '{topic}'")
    for p in profiles:
        print(f"  - {p['category']}: {p['url']} ({p['max_pages']} trang)")


if __name__ == "__main__":
    main()
