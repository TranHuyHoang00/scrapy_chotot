"""Đẩy 1 profile test vào Kafka topic input (dùng với Kafka dev).

Trên hệ thống thật, profile do Hydra scheduler đẩy vào topic
hydra-profile-scheduler-chotot; không cần chạy script này.

Profile trỏ tới trang tổng hợp CHOTOT_START_URL (mặc định https://www.chotot.com/mua-ban).

Cách dùng (chạy tại thư mục gốc dự án):
    python scripts/seed_profiles.py                 # mặc định CHOTOT_MAX_PAGES trang
    python scripts/seed_profiles.py --pages 2       # chỉ crawl 2 trang danh sách
    python scripts/seed_profiles.py --dry-run       # chỉ in ra
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

from chotot.utils.kafka_producer import close_producer, make_producer  # noqa: E402

TEST_TENANT = {"id": "test", "name": "Test", "project": "test", "category_ids": []}


def build_profile(url, max_pages, max_items_per_page):
    profile_id = hashlib.md5(url.encode("utf-8")).hexdigest()
    return {
        "profile_id": profile_id,
        "id": profile_id,
        "url": url,
        "seed_url": url,
        "name": "chotot",
        "type": "page",
        "channel_name": "chotot",
        "tenants": [TEST_TENANT],
        "max_pages": max_pages,
        "max_items_per_page": max_items_per_page,
    }


def main():
    parser = argparse.ArgumentParser(description="Đẩy profile test vào Kafka topic input")
    parser.add_argument("--pages", type=int, help="số trang danh sách cần crawl")
    parser.add_argument("--items", type=int, default=0, help="số tin tối đa mỗi trang, 0 = lấy hết")
    parser.add_argument("--dry-run", action="store_true", help="chỉ in ra, không đẩy")
    args = parser.parse_args()

    settings = get_project_settings()
    topic = settings.get("KAFKA_TOPIC_INPUT")
    profile = build_profile(
        settings.get("CHOTOT_START_URL"),
        args.pages or settings.getint("CHOTOT_MAX_PAGES"),
        args.items,
    )

    if args.dry_run:
        print(json.dumps(profile, ensure_ascii=False, indent=2))
        print(f"\n[dry-run] profile sẽ được đẩy vào topic '{topic}'")
        return

    producer = make_producer(settings)
    producer.send(topic, value=profile, key=profile["profile_id"])
    close_producer(producer)
    print(f"Đã đẩy profile {profile['url']} ({profile['max_pages']} trang) vào topic '{topic}'")


if __name__ == "__main__":
    main()
