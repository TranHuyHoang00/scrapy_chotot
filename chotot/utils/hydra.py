"""Các hàm dùng chung theo quy ước Hydra."""

import hashlib
from datetime import datetime, timezone


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def iso_to_ms(value):
    """Đổi chuỗi ISO 8601 sang timestamp mili giây; giá trị khác giữ nguyên."""
    if not isinstance(value, str) or not value:
        return value
    try:
        return int(datetime.fromisoformat(value).timestamp() * 1000)
    except ValueError:
        return value


def generate_key(unique_string):
    """Giống generateKey() của spider mẫu: MD5 → số nguyên → 27 ký tự đầu."""
    md5_hex = hashlib.md5(str(unique_string).encode("utf-8")).hexdigest()
    return str(int(md5_hex, 16))[:27]


def get_ad(item):
    """Lấy phần dữ liệu tin (AdItem) bên trong item Hydra {"item_data": {"data": ...}}."""
    if isinstance(item, dict) and "item_data" in item:
        return item["item_data"]["data"]
    return item
