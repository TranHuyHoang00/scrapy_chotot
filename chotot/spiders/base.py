"""Spider nền theo quy ước Hydra (giống spider mẫu opensanctions).

- Nguồn crawl: đọc profile từ Kafka topic KAFKA_TOPIC_INPUT (hydra-profile-scheduler-<tên>).
- Ghi crawled_time vào Elasticsearch, cập nhật source_status qua API profile.
- Log có tag [HYDRA-MONITOR] được gửi sang Kafka để giám sát.

Spider con chỉ cần viết start_profile() và các hàm parse.
"""

import json
import logging
import time

import requests
import scrapy
import urllib3
from scrapy import signals

from chotot.kafka_logging import KafkaLoggingHandler
from chotot.utils.hydra import make_test_profile, now_ms
from chotot.utils.kafka_producer import make_consumer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HydraBaseSpider(scrapy.Spider):
    name = None

    def __init__(self, proxy=None, topic=None, ttl=None, domain=None, check_lag=None, start_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arg_start_url = start_url
        self.arg_proxy = proxy
        self.arg_topic = topic
        self.ttl = ttl
        self.domain = domain
        self.check_lag = bool(check_lag)
        self.queue = []
        self.status_sent = set()

    # ------------------------------------------------------------------ setup

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.setup_hydra()
        crawler.signals.connect(spider.on_spider_closed, signal=signals.spider_closed)
        return spider

    def setup_hydra(self):
        s = self.settings
        self.topic = self.arg_topic or s.get("KAFKA_TOPIC_INPUT")
        self.my_proxy = self.arg_proxy or (s.get("CRAWL_PROXY") if s.getbool("USE_PROXY") else "")
        self.end_point_api = (s.get("END_POINT_API") or "").rstrip("/")
        self.api_key = s.get("API_KEY_GLOBAL")

        self.es = None
        if s.get("ES_HOST"):
            from elasticsearch import Elasticsearch

            self.es = Elasticsearch(s.get("ES_HOST"), request_timeout=30, max_retries=3, retry_on_timeout=True)

        self.kafka_log_handler = None
        if s.getbool("KAFKA_LOGGING_ENABLED", True):
            self.kafka_log_handler = KafkaLoggingHandler(s)
            logging.getLogger().addHandler(self.kafka_log_handler)

    def on_spider_closed(self, spider, reason):
        self.monitor("INFO", f"Crawl finished: {reason}")
        if self.kafka_log_handler:
            logging.getLogger().removeHandler(self.kafka_log_handler)
            self.kafka_log_handler.close()

    # ---------------------------------------------------------------- logging

    def monitor(self, level, message, tag=None):
        """Log theo định dạng [HYDRA-MONITOR] [tên] [TAG] nội dung."""
        tag = tag or level
        getattr(self.logger, level.lower())(f"[HYDRA-MONITOR] [{self.name}] [{tag}] {message}")

    # ----------------------------------------------------------------- source

    def get_source(self):
        """Đọc profile cần crawl từ Kafka, trả về list dict."""
        self.monitor("INFO", "GET SOURCE", tag="START")
        self.queue = []
        try:
            consumer = make_consumer(self.settings)
            consumer.subscribe([self.topic])
            messages = consumer.poll(
                timeout_ms=self.settings.getint("KAFKA_POLL_TIMEOUT_MS", 50000),
                max_records=self.settings.getint("KAFKA_POLL_MAX_RECORDS", 500),
            )
            for records in messages.values():
                for record in records:
                    try:
                        self.queue.append(json.loads(record.value))
                    except (TypeError, ValueError):
                        self.monitor("ERROR", f"Message không phải JSON: {record.value[:200]!r}")
            if self.check_lag:
                self.log_lag(consumer)
            time.sleep(1)
            consumer.close()
        except Exception as e:
            self.monitor("ERROR", f"[{self.topic}] {e}")
        return self.queue

    def log_lag(self, consumer):
        try:
            partitions = list(consumer.assignment())
            end_offsets = consumer.end_offsets(partitions)
            lag = sum(end_offsets[p] - consumer.position(p) for p in partitions)
            self.monitor("INFO", f"[Topic:{self.topic}] Lag còn lại: {lag}", tag="LAG")
        except Exception as e:
            self.monitor("ERROR", f"Không tính được lag: {e}")

    def load_profiles(self):
        """Profile cần crawl.
        - Có -a start_url=...: chạy test với 1 profile giả, không đọc Kafka.
        - KAFKA_MODE=http: API chỉ đẩy được, không đọc được topic input → dùng CHOTOT_START_URL.
        - Còn lại: đọc topic input như Hydra."""
        if self.arg_start_url:
            self.monitor("INFO", f"Chạy với start_url, không đọc Kafka input: {self.arg_start_url}")
            return [make_test_profile(self.arg_start_url, name=self.name)]
        if (self.settings.get("KAFKA_MODE") or "").lower() == "http":
            url = self.settings.get("CHOTOT_START_URL")
            self.monitor("WARNING", f"KAFKA_MODE=http không đọc được topic input, dùng CHOTOT_START_URL: {url}")
            return [make_test_profile(url, name=self.name)]
        return self.get_source()

    def start_requests(self):
        profiles = self.load_profiles()
        self.monitor("INFO", f"[Topic:{self.topic}] Total source: {len(profiles)}", tag="ITEM")

        for profile in profiles:
            url = profile.get("seed_url") or profile.get("url") or ""
            profile_id = profile.get("profile_id", "")
            if not url.startswith(("http://", "https://")):
                self.monitor("INFO", f'[{profile_id}] No "http://" or "https://" in url: {url}')
                self.update_status_source(profile_id, ["No http/https"])
                continue
            self.sent_time_crawl(profile)
            yield from self.start_profile(profile, url)

    def start_profile(self, profile, url):
        raise NotImplementedError(f"{self.name}: chưa viết start_profile()")

    def request_meta(self, profile, **extra):
        meta = {"profile": profile, **extra}
        if self.my_proxy:
            meta["proxy"] = self.my_proxy
        return meta

    # ------------------------------------------------------------- envelope

    def build_item_data(self, profile, url, post_id, data):
        """Bọc dữ liệu thành item Hydra. created_time/updated_time/entry_rowkey...
        được điền ở pipeline cuối, sau khi dữ liệu đã chuẩn hóa."""
        tenants = profile.get("tenants") or []
        return {
            "item_data": {
                "url": url,
                "data": data,
                "tenant_ids": [t.get("id") for t in tenants if isinstance(t, dict)],
                "_tenants": tenants,
                "docType": "Post",
                "type": "post",
                "channel_name": profile.get("channel_name") or self.name,
                "post_id": post_id,
                "profile_id": profile.get("profile_id", ""),
                "owner_name": profile.get("name", ""),
                "author_name": profile.get("name", ""),
                "owner_type": profile.get("type", ""),
                # TODO: xác nhận với team giá trị crawler_system cho spider crawl HTML
                "crawler_system": self.settings.get("HYDRA_CRAWLER_SYSTEM", "api-crawler"),
            }
        }

    # ---------------------------------------------------- elasticsearch / API

    def sent_time_crawl(self, profile):
        if self.es is None:
            return
        profile_id = profile.get("profile_id")
        if not profile_id:
            return
        doc = {"crawled_time": now_ms()}
        if "update_last_updated_time" in profile:
            doc["last_updated_time"] = profile["update_last_updated_time"]
        for index in (self.settings.get("ES_INDEX_CRAWLED_TIME"), "hydra-profile"):
            if not index:
                continue
            try:
                self.es.update(index=index, id=profile_id, doc=doc)
                return
            except Exception:
                continue
        self.logger.error(f"Update crawled time to elasticsearch profile id: {profile_id} fail")

    def update_status_source(self, profile_id, source_status, once=False):
        """PATCH source_status của profile. once=True: mỗi profile chỉ gửi 1 lần/lượt chạy."""
        if not profile_id or not self.end_point_api:
            return
        if once:
            key = (profile_id, tuple(source_status))
            if key in self.status_sent:
                return
            self.status_sent.add(key)
        try:
            res = requests.patch(
                f"{self.end_point_api}/profiles/{profile_id}",
                params={"apikey": self.api_key},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"source_status": source_status}),
                verify=False,
                proxies={"http": "", "https": ""},
                timeout=30,
            )
            self.logger.info(f"Update source status profile: {profile_id} status: {res.status_code}")
        except Exception as e:
            self.logger.error(f"Update source status profile: {profile_id} fail: {e}")

    def error_back_start_request(self, failure):
        """Trang đầu tiên của profile lỗi 4xx → đánh dấu profile dead."""
        request = failure.request
        profile = request.meta.get("profile", {})
        status = getattr(getattr(failure.value, "response", None), "status", None)
        self.monitor("ERROR", f"[{profile.get('profile_id')}] Request lỗi {request.url}: {failure.getErrorMessage()}")
        if status and 400 <= status < 500:
            self.update_status_source(profile.get("profile_id"), ["dead"], once=True)
