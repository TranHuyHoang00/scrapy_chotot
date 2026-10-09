"""Các hàm làm sạch và chuẩn hóa dữ liệu dùng chung."""

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib.parse import urldefrag

VN_TZ = timezone(timedelta(hours=7))

_ZERO_WIDTH = re.compile(r"[\u200b-\u200d\ufeff]")
_SPACES = re.compile(r"\s+")


def clean_text(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = " ".join(str(v) for v in value if v)
    value = unicodedata.normalize("NFC", str(value))
    value = _ZERO_WIDTH.sub("", value)
    value = _SPACES.sub(" ", value).strip()
    return value or None


def clean_multiline(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = "\n".join(str(v) for v in value if v)
    value = unicodedata.normalize("NFC", str(value))
    value = _ZERO_WIDTH.sub("", value)
    lines = [_SPACES.sub(" ", line).strip() for line in value.splitlines()]
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    return text or None


def remove_accents(text):
    text = text.replace("đ", "d").replace("Đ", "D")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def slugify_key(label):
    text = remove_accents(clean_text(label) or "").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or None

def clean_url(url):
    return urldefrag(url)[0] if url else None


def extract_id(url, pattern=r"/(\d+)\.htm"):
    match = re.search(pattern, url or "")
    return match.group(1) if match else None


_PRICE_UNITS = {
    "tỷ": 1_000_000_000, "ty": 1_000_000_000,
    "triệu": 1_000_000, "trieu": 1_000_000, "tr": 1_000_000,
    "nghìn": 1_000, "ngàn": 1_000, "k": 1_000,
}
_PRICE_UNIT_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(tỷ|ty|triệu|trieu|tr|nghìn|ngàn|k)\b"
)
_NO_PRICE_WORDS = ("thỏa thuận", "thoả thuận", "liên hệ")


def parse_price(text):
    text = clean_text(text)
    if not text:
        return None
    low = text.lower()
    if any(word in low for word in _NO_PRICE_WORDS):
        return None

    matches = _PRICE_UNIT_RE.findall(low)
    if matches:
        total = sum(float(num.replace(",", ".")) * _PRICE_UNITS[unit] for num, unit in matches)
        return int(round(total))

    digits = re.sub(r"\D", "", low)
    return int(digits) if digits else None


def parse_number(text):
    text = clean_text(text)
    if not text:
        return None
    match = re.search(r"\d+(?:[.,]\d+)?", text)
    if not match:
        return None
    number = float(match.group().replace(",", "."))
    return int(number) if number.is_integer() else number


_RELATIVE_UNITS = {
    "giây": timedelta(seconds=1),
    "phút": timedelta(minutes=1),
    "giờ": timedelta(hours=1),
    "ngày": timedelta(days=1),
    "tuần": timedelta(weeks=1),
    "tháng": timedelta(days=30),
    "năm": timedelta(days=365),
}
_RELATIVE_RE = re.compile(r"(\d+)\s*(giây|phút|giờ|ngày|tuần|tháng|năm)\s*trước")
_ABSOLUTE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?")


def to_iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_posted_time(text, now=None):
    text = clean_text(text)
    if not text:
        return None
    low = text.lower()
    now = now or datetime.now(VN_TZ)

    match = _RELATIVE_RE.search(low)
    if match:
        return to_iso_utc(now - int(match.group(1)) * _RELATIVE_UNITS[match.group(2)])
    if "hôm qua" in low:
        return to_iso_utc(now - timedelta(days=1))
    if "vừa" in low or "hôm nay" in low:
        return to_iso_utc(now)

    match = _ABSOLUTE_RE.search(low)
    if match:
        day, month, year, hour, minute = match.groups()
        dt = datetime(int(year), int(month), int(day),
                      int(hour or 0), int(minute or 0), tzinfo=VN_TZ)
        return to_iso_utc(dt)

    return None

def split_location(text):
    text = clean_text(text)
    if not text:
        return None, None
    parts = [p for p in (clean_text(x) for x in text.split(",")) if p]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[0]