import logging
import uuid
from datetime import datetime, timezone

from itemadapter import ItemAdapter

from chotot.items import AdItem
from chotot.pipelines.redis_dedup import unmark
from chotot.utils.kafka_producer import close_producer, default_topic, get_producer
from chotot.utils.normalizer import to_iso_utc

SCHEMA_VERSION = "1.0"
EVENT_TYPE = "ad.upsert"
LIST_FIELDS = ("image_urls",)
DICT_FIELDS = ("specs",)

logger = logging.getLogger(__name__)


def build_message(item):
    adapter = ItemAdapter(item)
    data = {}
    for field in AdItem.fields:
        value = adapter.get(field)
        if value is None and field in LIST_FIELDS:
            value = []
        if value is None and field in DICT_FIELDS:
            value = {}
        data[field] = value

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "event_type": EVENT_TYPE,
        "event_time": to_iso_utc(datetime.now(timezone.utc)),
        "source": data["source"],
        "data": data,
    }


def message_key(message):
    return f"{message['source']}:{message['data']['ad_id']}"


class KafkaPipeline:
    def __init__(self, topic, stats=None):
        self.topic = topic
        self.stats = stats
        self.producer = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            topic=crawler.settings.get("KAFKA_TOPIC") or default_topic(),
            stats=crawler.stats,
        )

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"kafka/{name}")

    def open_spider(self):
        self.producer = get_producer()
        logger.info("Kafka topic: %s", self.topic)

    def process_item(self, item):
        if self.producer is None:
            self.producer = get_producer()

        message = build_message(item)
        future = self.producer.send(self.topic, value=message, key=message_key(message))
        future.add_callback(self._on_success)
        future.add_errback(self._on_error, message["source"], message["data"]["ad_id"])
        self._inc("sent")
        return item

    def _on_success(self, metadata):
        self._inc("delivered")

    def _on_error(self, source, ad_id, exc):
        self._inc("failed")
        unmark(source, ad_id)
        logger.error("Gui Kafka that bai %s:%s - %s", source, ad_id, exc)

    def close_spider(self):
        close_producer()
        self.producer = None
        logger.info("Da dong Kafka producer")