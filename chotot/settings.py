"""Settings theo cấu trúc spider Hydra.

Giá trị máy chủ, key, mật khẩu KHÔNG ghi trong code: đặt qua biến môi trường
(Crawlab: phần Environment của spider) hoặc file .env khi chạy local.
Xem .env.example để biết danh sách biến.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

from chotot.utils.redis_client import redis_url

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def env(name, default=None):
    value = os.getenv(name)
    return default if value in (None, "") else value


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    return int(env(name, default))


def env_float(name, default):
    return float(env(name, default))


def env_list(name, default):
    return [s.strip() for s in env(name, default).split(",") if s.strip()]


# ===== Scrapy =====
BOT_NAME = "chotot"
SPIDER_MODULES = ["chotot.spiders"]
NEWSPIDER_MODULE = "chotot.spiders"

ROBOTSTXT_OBEY = False
COOKIES_ENABLED = False
TELNETCONSOLE_ENABLED = False

CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 8)
CONCURRENT_REQUESTS_PER_DOMAIN = env_int("CONCURRENT_REQUESTS_PER_DOMAIN", 4)
DOWNLOAD_DELAY = env_float("DOWNLOAD_DELAY", 1.5)
DOWNLOAD_TIMEOUT = 30

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504, 522, 524]

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "chotot.middlewares.RandomUserAgentMiddleware": 400,
    "chotot.middlewares.ProxyMiddleware": 410,
}

ITEM_PIPELINES = {
    "chotot.pipelines.normalize.NormalizePipeline": 100,
    "chotot.pipelines.validation.ValidationPipeline": 200,
    "chotot.pipelines.redis_dedup.RedisDedupPipeline": 300,
    "chotot.pipelines.kafka_pipeline.HydraKafkaPipeline": 400,
}

EXTENSIONS = {
    "chotot.extensions.CrawlMonitor": 500,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Dừng sau N item hợp lệ (0 = không giới hạn)
CLOSESPIDER_ITEMCOUNT = env_int("TARGET_ITEM_COUNT", 0)

# ===== Log =====
LOG_ENABLED = True
LOG_LEVEL = env("LOG_LEVEL", "INFO")
LOG_FORMAT = f"%(asctime)s [{BOT_NAME}] %(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
LOG_DIR = str(BASE_DIR / "logs")
MONITOR_ENABLED = env_bool("MONITOR_ENABLED", True)
MONITOR_PROGRESS_EVERY = env_int("MONITOR_PROGRESS_EVERY", 50)

# ===== Crawl chotot =====
CHOTOT_START_URL = env("CHOTOT_START_URL", "https://www.chotot.com/mua-ban")  # trang tổng hợp toàn bộ tin
CHOTOT_PAGE_PARAM = "page"
CHOTOT_MAX_PAGES = env_int("CHOTOT_MAX_PAGES", 5)                    # số trang/danh mục nếu profile không khai báo
CHOTOT_MAX_ITEMS_PER_PAGE = env_int("CHOTOT_MAX_ITEMS_PER_PAGE", 0)  # 0 = lấy hết

# Trường bắt buộc theo site; site không khai báo dùng "default"
# Không bắt buộc price: tin cho tặng, thỏa thuận, không ghi giá vẫn giữ (price = null)
VALIDATION_RULES = {
    "default": ["ad_id", "url", "title"],
}
VALIDATION_MIN_TITLE_LENGTH = 3
DEDUP_ALLOW_UPDATES = True

# ===== Proxy =====
USE_PROXY = env_bool("USE_PROXY", False)
CRAWL_PROXY = env("CRAWL_PROXY")

# ===== Kafka =====
# broker: kết nối thẳng Kafka (Hydra) | http: gửi qua API publish-message (Kafka dev)
KAFKA_MODE = env("KAFKA_MODE", "broker")
KAFKA_HTTP_URL = env("KAFKA_HTTP_URL")
KAFKA_HTTP_API_KEY = env("KAFKA_HTTP_API_KEY")
KAFKA_HTTP_BATCH_SIZE = env_int("KAFKA_HTTP_BATCH_SIZE", 50)
KAFKA_HTTP_TIMEOUT = env_int("KAFKA_HTTP_TIMEOUT", 30)
KAFKA_HTTP_VERIFY_SSL = env_bool("KAFKA_HTTP_VERIFY_SSL", True)

KAFKA_BOOTSTRAP_SERVERS = env_list("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "SSL")
KAFKA_SSL_CAFILE = env("KAFKA_SSL_CAFILE", "chotot/ssl/ca.pem")
KAFKA_SSL_KEYFILE = env("KAFKA_SSL_KEYFILE", "chotot/ssl/key.pem")
KAFKA_SSL_CERTFILE = env("KAFKA_SSL_CERTFILE", "chotot/ssl/certificate.pem")
KAFKA_SSL_CHECK_HOSTNAME = env_bool("KAFKA_SSL_CHECK_HOSTNAME", False)
KAFKA_API_VERSION = env("KAFKA_API_VERSION", "")  # vd 1.0.0 như spider mẫu; trống = tự dò
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "group-chotot")
KAFKA_AUTO_OFFSET_RESET = env("KAFKA_AUTO_OFFSET_RESET", "earliest")
KAFKA_POLL_TIMEOUT_MS = env_int("KAFKA_POLL_TIMEOUT_MS", 50000)
KAFKA_POLL_MAX_RECORDS = env_int("KAFKA_POLL_MAX_RECORDS", 500)

KAFKA_TOPIC_INPUT = env("KAFKA_TOPIC_INPUT", "hydra-profile-scheduler-chotot")
KAFKA_TOPIC_OUTPUT = env("KAFKA_TOPIC_OUTPUT", "hydra-chotot")
KAFKA_TOPIC_LOG = env("KAFKA_TOPIC_LOG", "hydra-spider-log")
KAFKA_LOGGING_ENABLED = env_bool("KAFKA_LOGGING_ENABLED", True)

# ===== Redis (chống trùng item) =====
REDIS_URL = redis_url()

# ===== Elasticsearch / API profile =====
ES_HOST = env("ES_HOST")                              # trống = bỏ qua cập nhật crawled_time
ES_INDEX_CRAWLED_TIME = env("ES_INDEX_CRAWLED_TIME", "hydra-profiles")
END_POINT_API = env("END_POINT_API")                  # trống = bỏ qua cập nhật source_status
API_KEY_GLOBAL = env("API_KEY_GLOBAL")
HYDRA_CRAWLER_SYSTEM = env("HYDRA_CRAWLER_SYSTEM", "api-crawler")
