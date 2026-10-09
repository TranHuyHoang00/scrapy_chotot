import logging
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import NotConfigured

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
NOISY_LOGGERS = ("kafka",)
REASONS = {
    "finished": "hết URL cần crawl",
    "closespider_itemcount": "đủ số item mục tiêu",
    "closespider_timeout": "hết thời gian cho phép",
    "shutdown": "người dùng dừng",
}


def mask_proxy(proxy):
    if not proxy:
        return "không dùng"
    parsed = urlparse(proxy)
    return f"{parsed.scheme}://***:{parsed.port}" if parsed.port else "***"


def mask_url(url):
    return re.sub(r"//[^@/]*@", "//***@", url or "")


class CrawlMonitor:
    def __init__(self, crawler, log_dir, progress_every):
        self.crawler = crawler
        self.settings = crawler.settings
        self.stats = crawler.stats
        self.log_dir = Path(log_dir)
        self.progress_every = progress_every
        self.logger = logging.getLogger("monitor")
        self.handler = None
        self.log_file = None
        self.started = None
        self.items = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool("MONITOR_ENABLED", True):
            raise NotConfigured
        ext = cls(
            crawler,
            settings.get("LOG_DIR", "logs"),
            settings.getint("MONITOR_PROGRESS_EVERY", 50),
        )
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)
        return ext

    def _open_file_log(self, spider):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / f"{spider.name}_{datetime.now():%Y%m%d_%H%M%S}.log"
        self.handler = logging.FileHandler(self.log_file, encoding="utf-8")
        self.handler.setFormatter(logging.Formatter(FILE_FORMAT, DATE_FORMAT))
        self.handler.setLevel(self.settings.get("LOG_LEVEL", "INFO"))
        logging.getLogger().addHandler(self.handler)
        for name in NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)

    def _close_file_log(self):
        if self.handler:
            logging.getLogger().removeHandler(self.handler)
            self.handler.close()
            self.handler = None

    def spider_opened(self, spider):
        self.started = time.time()
        self._open_file_log(spider)
        s = self.settings
        target = s.getint("CLOSESPIDER_ITEMCOUNT")
        proxy = mask_proxy(s.get("CRAWL_PROXY")) if s.getbool("USE_PROXY") else "không dùng"

        self.logger.info("===== Bắt đầu spider '%s' =====", spider.name)
        self.logger.info("Ghi log vào: %s", self.log_file)
        self.logger.info("Mục tiêu: %s", f"{target} item hợp lệ" if target else "không giới hạn")
        self.logger.info("Proxy: %s", proxy)
        self.logger.info(
            "Tốc độ: delay %ss, %s request song song/domain",
            s.getfloat("DOWNLOAD_DELAY"),
            s.getint("CONCURRENT_REQUESTS_PER_DOMAIN"),
        )
        self.logger.info("Redis: %s | Kafka topic: %s", mask_url(s.get("REDIS_URL")), s.get("KAFKA_TOPIC"))

    def item_scraped(self, item, response, spider):
        self.items += 1
        category = item.get("category") or "unknown"
        self.stats.inc_value(f"monitor/category/{category}")

        if self.progress_every and self.items % self.progress_every == 0:
            target = self.settings.getint("CLOSESPIDER_ITEMCOUNT")
            minutes = max((time.time() - self.started) / 60, 1 / 60)
            self.logger.info(
                "Tiến độ: %d/%s item hợp lệ (%.1f item/phút)",
                self.items, target or "?", self.items / minutes,
            )

    def item_dropped(self, item, response, exception, spider):
        self.logger.debug("Bỏ item: %s", exception)

    def _get(self, key):
        return self.stats.get_value(key, 0)

    def _with_prefix(self, prefix):
        return sorted(
            (k[len(prefix):], v) for k, v in self.stats.get_stats().items() if k.startswith(prefix)
        )

    def spider_closed(self, spider, reason):
        elapsed = time.time() - (self.started or time.time())
        target = self.settings.getint("CLOSESPIDER_ITEMCOUNT")
        valid = self._get("item_scraped_count")
        sent, delivered, failed = self._get("kafka/sent"), self._get("kafka/delivered"), self._get("kafka/failed")
        forbidden = self._get("robotstxt/forbidden")

        self.logger.info(
            "===== Kết thúc spider '%s' sau %.1fs (%s) =====",
            spider.name, elapsed, REASONS.get(reason, reason),
        )
        self.logger.info(
            "Request: %d gửi, %d phản hồi, %d thử lại, %d bị robots.txt chặn",
            self._get("downloader/request_count"), self._get("response_received_count"),
            self._get("retry/count"), forbidden,
        )
        self.logger.info("Item: %d hợp lệ, %d bị loại", valid, self._get("validation/dropped"))
        for name, count in self._with_prefix("validation/dropped/"):
            self.logger.info("  - Loại vì %s: %d", name, count)
        self.logger.info(
            "Chống trùng: %d mới, %d cập nhật, %d trùng",
            self._get("dedup/new"), self._get("dedup/updated"), self._get("dedup/duplicate"),
        )
        self.logger.info("Kafka: %d gửi, %d thành công, %d thất bại", sent, delivered, failed)

        categories = self._with_prefix("monitor/category/")
        if categories:
            self.logger.info("Theo danh mục:")
            for name, count in categories:
                self.logger.info("  - %s: %d", name, count)

        if forbidden:
            self.logger.warning("Có %d URL bị robots.txt chặn, kiểm tra lại nguồn URL", forbidden)
        if sent != delivered + failed:
            self.logger.warning("Còn %d message Kafka chưa có kết quả", sent - delivered - failed)
        if failed:
            self.logger.warning("Có %d message gửi Kafka thất bại, sẽ được gửi lại ở lần crawl sau", failed)
        if target and valid < target:
            self.logger.warning("Chưa đạt mục tiêu: %d/%d item hợp lệ", valid, target)
        if not valid:
            self.logger.error("Không thu được item hợp lệ nào")

        self.logger.info("Log đầy đủ: %s", self.log_file)
        self._close_file_log()