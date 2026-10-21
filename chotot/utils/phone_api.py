"""Lấy số điện thoại đầy đủ của tin qua API Chợ Tốt.

    GET PHONE_API_URL?e=<ad_id>           (ad_id = list_id, ID trên URL tin: .../135193883.htm)
    Header: Authorization: Bearer PHONE_API_TOKEN
    Response: {"phone": "0523149999"}

Dùng chung cho mọi site (chotot, xe, nhatot, vieclamtot).
Request đi qua Scrapy (bất đồng bộ, không chặn crawl) nên tự theo proxy USE_PROXY / CRAWL_PROXY.

Mọi thứ liên quan đến API nằm trong file này: đổi API thì chỉ sửa ở đây.

Kết quả:
    - Có số: trả về chuỗi số, lưu cache Redis (PHONE_CACHE_TTL_HOURS).
    - Không có số / lỗi mạng / lỗi HTTP khác: trả về None, tin vẫn được gửi với phone = null.
    - 401/403 (token hết hạn hoặc sai): raise PhoneAuthError → spider dừng và báo lỗi.
"""

import json
import logging

import scrapy
from w3lib.url import add_or_replace_parameter

from chotot.utils.redis_client import get_redis

import base64
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_public_key

AUTH_ERROR_CODES = (401, 403)
DEFAULT_URL = "https://gateway.chotot.com/v1/private/ad-listing/phone"
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.chotot.com",
    "Referer": "https://www.chotot.com/",
}

PROD_PUBKEY = b"""-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAxnvPjlA/K/adq6mA6+uU
tlyBBxFaKeK+WD2FypOeCAP0qtucmaDrIbxirykrxQjRpGxl2HKRBwGd2h/hDuk9
CxRUXD2p0Hrzb1Hb9M5px19TPXM6AWSClR1kozehRusIFrxP6PHqDLx5prJFLlSZ
zg3N3oGhS6oP/a4Ku/iAdCUCiHb5TX3b3+y4Ll/QViZhpKZjU6BhIOsiVIJhyXvn
0cSqLXPjNuXR5A4JkmRl9T9cWncEHTKmoVUyXQJaDZa3yH/OJSEmhhGyKNKkM5so
lasJWSBKenFnFvphw3+KG8BGfJwGkvtRAVbS1ljduH8z8fxALxHgUdnTtgpxB+KZ
/CVnNr97EGqYPLVlX+duGkuy1yCunqVTiY2HyL/0bMTBK84oCQjtMVAHgZ345hZn
mGST71D8+i5HGtOOFoRyP6qK6ex1qfEROzWsmVDA00aHLlQcKOLaHvT/DB30aeUs
ZoL/kQo100XccufpHESrits0mEuoyza4CCFM04F3pDOXAgMBAAE=
-----END PUBLIC KEY-----"""

_key = load_pem_public_key(PROD_PUBKEY)

logger = logging.getLogger(__name__)


class PhoneAuthError(Exception):
    """Token API phone hết hạn hoặc không hợp lệ (HTTP 401/403)."""

def make_e(ad_id: str) -> str:
    ct = _key.encrypt(str(ad_id).encode(), padding.PKCS1v15())
    return base64.b64encode(ct).decode()

def cache_key(site, ad_id):
    return f"phone:{site}:{ad_id}"


def clean_phone(value):
    if value is None:
        return None
    phone = str(value).strip().replace(" ", "").replace(".", "")
    return phone or None




class PhoneApi:
    def __init__(self, url, token, timeout=15, cache_ttl_hours=0, crawler=None):
        if not token:
            raise ValueError("PHONE_API_ENABLED=true cần PHONE_API_TOKEN trong .env")
        self.url = url or DEFAULT_URL
        self.token = token
        self.timeout = timeout
        self.cache_ttl = max(0, int(cache_ttl_hours)) * 3600
        # Giữ crawler, không lấy crawler.stats ngay: từ Scrapy 2.19, stats chưa có
        # lúc tạo spider (from_crawler), chỉ có khi crawl đã bắt đầu
        self.crawler = crawler

    @property
    def stats(self):
        try:
            return self.crawler.stats if self.crawler else None
        except RuntimeError:
            return None

    @classmethod
    def from_crawler(cls, crawler):
        """Trả về None nếu PHONE_API_ENABLED=false."""
        s = crawler.settings
        if not s.getbool("PHONE_API_ENABLED", True):
            return None
        return cls(
            url=s.get("PHONE_API_URL"),
            token=s.get("PHONE_API_TOKEN"),
            timeout=s.getint("PHONE_API_TIMEOUT", 15),
            cache_ttl_hours=s.getint("PHONE_CACHE_TTL_HOURS", 0),
            crawler=crawler,
        )

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"phone_api/{name}")

    # --------------------------------------------------------------- cache

    def get_cached(self, site, ad_id):
        if not self.cache_ttl:
            return None
        try:
            phone = get_redis().get(cache_key(site, ad_id))
        except Exception as e:  # Redis lỗi thì gọi API như bình thường
            logger.warning("Đọc cache phone lỗi: %s", e)
            return None
        if phone:
            self._inc("cache_hit")
        return phone or None

    def set_cached(self, site, ad_id, phone):
        if not self.cache_ttl or not phone:
            return
        try:
            get_redis().set(cache_key(site, ad_id), phone, ex=self.cache_ttl)
        except Exception as e:
            logger.warning("Ghi cache phone lỗi: %s", e)

    # ------------------------------------------------------------- request

    def build_request(self, ad_id, callback, errback, meta=None):
        meta = dict(meta or {})
        meta.update(
            phone_ad_id=str(ad_id),
            download_timeout=self.timeout,
            handle_httpstatus_all=True,  # tự xử lý 401/403/404 trong callback
        )
        self._inc("request")
        return scrapy.Request(
            add_or_replace_parameter(self.url, "e", str(make_e(ad_id))),
            headers={**DEFAULT_HEADERS, "Authorization": f"Bearer {self.token}"},
            callback=callback,
            errback=errback,
            meta=meta,
            dont_filter=True,
        )

    def parse_response(self, response, site):
        """Trả về số điện thoại hoặc None. Raise PhoneAuthError khi 401/403."""
        ad_id = response.meta.get("phone_ad_id")
        status = response.status

        if status in AUTH_ERROR_CODES:
            self._inc(f"http_{status}")
            raise PhoneAuthError(f"HTTP {status}: {response.text[:200]}")

        if status != 200:
            self._inc(f"http_{status}")
            self._inc("error")
            logger.warning("API phone %s:%s trả về HTTP %s", site, ad_id, status)
            return None

        try:
            data = json.loads(response.text)
        except ValueError:
            self._inc("bad_json")
            self._inc("error")
            logger.warning("API phone %s:%s trả về không phải JSON: %r", site, ad_id, response.text[:200])
            return None

        phone = clean_phone(data.get("phone")) if isinstance(data, dict) else None
        if not phone:
            self._inc("no_phone")
            return None

        self._inc("ok")
        self.set_cached(site, ad_id, phone)
        return phone

    def on_failure(self, failure, site):
        """Lỗi mạng, timeout (sau khi đã retry). Tin vẫn được gửi với phone = null."""
        self._inc("error")
        ad_id = failure.request.meta.get("phone_ad_id")
        logger.warning("Gọi API phone %s:%s lỗi: %s", site, ad_id, failure.getErrorMessage())
