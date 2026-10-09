import os
from pathlib import Path

from dotenv import load_dotenv

from hb_1014.utils.redis_client import redis_url

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    return int(os.getenv(name, default))


def env_float(name, default):
    return float(os.getenv(name, default))


BOT_NAME = "hb_1014"
SPIDER_MODULES = ["hb_1014.spiders"]
NEWSPIDER_MODULE = "hb_1014.spiders"

# ROBOTSTXT_OBEY = True
ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 8)
DOWNLOAD_DELAY = env_float("DOWNLOAD_DELAY", 1)
RANDOMIZE_DOWNLOAD_DELAY = 0.5
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

USE_PROXY = env_bool("USE_PROXY", False)
CRAWL_PROXY = os.getenv("CRAWL_PROXY")

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "hb_1014.middlewares.RandomUserAgentMiddleware": 400,
    "hb_1014.middlewares.ProxyMiddleware": 410,
}

ITEM_PIPELINES = {
    "hb_1014.pipelines.normalize.NormalizePipeline": 100,
    "hb_1014.pipelines.validation.ValidationPipeline": 200,
    "hb_1014.pipelines.redis_dedup.RedisDedupPipeline": 300,
    "hb_1014.pipelines.kafka_pipeline.KafkaPipeline": 400,
}

VALIDATION_REQUIRED_FIELDS = ["ad_id", "url", "vertical", "category", "title", "price"]
VALIDATION_MIN_TITLE_LENGTH = 3
DEDUP_ALLOW_UPDATES = True

SCHEDULER = "scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.PriorityQueue"
SCHEDULER_PERSIST = True
REDIS_URL = redis_url()
REDIS_START_URLS_AS_SET = False
MAX_IDLE_TIME_BEFORE_CLOSE = env_int("MAX_IDLE_TIME", 60)

CLOSESPIDER_ITEMCOUNT = env_int("TARGET_ITEM_COUNT", 0)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FEED_EXPORT_ENCODING = "utf-8"

LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
LOG_DIR = str(BASE_DIR / "logs")

EXTENSIONS = {
    "hb_1014.extensions.CrawlMonitor": 500,
}
MONITOR_ENABLED = True
MONITOR_PROGRESS_EVERY = env_int("MONITOR_PROGRESS_EVERY", 50)