"""Gửi item sang Kafka theo format Hydra (giống opensanctionsPipeline của spider mẫu).

Message gửi đi là nội dung item["item_data"]:
    url, data, post_id, profile_id, entry_rowkey, tenant_ids, _tenants,
    created_time, updated_time, inserted_time, flag_crawl_created_time,
    docType, type, channel_name, owner_name, author_name, owner_type, crawler_system
Thời gian là timestamp mili giây.
"""

import logging

from itemadapter import ItemAdapter

from chotot.items import AdItem
from chotot.pipelines.redis_dedup import unmark
from chotot.utils.hydra import generate_key, iso_to_ms, now_ms
from chotot.utils.kafka_sender import make_sender

LIST_FIELDS = ("images", "videos")
DICT_FIELDS = ("specs", "specs_labels", "location", "seller")
# posted_at, updated_at đã là ms từ parser; giữ iso_to_ms phòng khi là chuỗi ISO
TIME_FIELDS = ("posted_at", "updated_at", "crawled_at")

logger = logging.getLogger(__name__)


def build_data(ad):
    """AdItem → dict đủ trường, thời gian đổi sang mili giây."""
    adapter = ItemAdapter(ad)
    data = {}
    for field in AdItem.fields:
        value = adapter.get(field)
        if value is None and field in LIST_FIELDS:
            value = []
        if value is None and field in DICT_FIELDS:
            value = {}
        if field in TIME_FIELDS:
            value = iso_to_ms(value)
        data[field] = value
    return data


def build_message(item_data):
    message = dict(item_data)
    data = build_data(item_data["data"])
    current = now_ms()

    created_time = data.get("posted_at")
    flag_crawl_created_time = 1 if isinstance(created_time, int) else 0
    if not flag_crawl_created_time:
        created_time = current
    updated_time = data.get("updated_at")
    if not isinstance(updated_time, int):
        updated_time = current

    message.update(
        url=data.get("url") or message.get("url"),
        data=data,
        created_time=created_time,
        flag_crawl_created_time=flag_crawl_created_time,
        updated_time=updated_time,
        inserted_time=current,
        entry_rowkey=generate_key(message["post_id"]),
    )
    return message


class HydraKafkaPipeline:
    def __init__(self, settings, stats=None):
        self.settings = settings
        self.topic = settings.get("KAFKA_TOPIC_OUTPUT")
        self.stats = stats
        self.sender = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, stats=crawler.stats)

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"kafka/{name}")

    def open_spider(self, spider=None):
        self.sender = make_sender(self.settings)
        logging.getLogger("kafka").setLevel(logging.ERROR)
        logger.info("Kafka (%s) topic output: %s", self.settings.get("KAFKA_MODE", "broker"), self.topic)

    def process_item(self, item, spider=None):
        if "item_data" not in item:
            return item
        if self.sender is None:
            self.sender = make_sender(self.settings)

        message = build_message(item["item_data"])
        site = message["data"].get("site")
        post_id = message["post_id"]

        self._inc("sent")
        self.sender.send(
            self.topic,
            message,
            key=post_id,
            on_success=self._on_success,
            on_error=lambda exc: self._on_error(site, post_id, exc),
        )
        return item

    def _on_success(self):
        self._inc("delivered")

    def _on_error(self, site, post_id, exc):
        self._inc("failed")
        unmark(site, post_id)  # bỏ dấu chống trùng để lần sau gửi lại
        logger.error("[HYDRA-MONITOR] Gửi Kafka thất bại %s:%s - %s", site, post_id, exc)

    def close_spider(self, spider=None):
        if self.sender is not None:
            self.sender.close()  # http: gửi nốt các message còn trong lô
        self.sender = None
        logger.info("Đã đóng Kafka sender")
