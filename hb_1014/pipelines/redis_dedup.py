import hashlib
import json

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from hb_1014.utils.redis_client import get_redis

IGNORED_FIELDS = ("crawled_at", "posted_at")


def dedup_key(source):
    return f"dedup:{source}"


def fingerprint(adapter):
    data = {k: v for k, v in adapter.asdict().items() if k not in IGNORED_FIELDS}
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def unmark(source, ad_id):
    get_redis().hdel(dedup_key(source), str(ad_id))


class RedisDedupPipeline:
    def __init__(self, allow_updates=True, stats=None):
        self.allow_updates = allow_updates
        self.stats = stats
        self.redis = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            allow_updates=crawler.settings.getbool("DEDUP_ALLOW_UPDATES", True),
            stats=crawler.stats,
        )

    def open_spider(self, spider):
        self.redis = get_redis()
        self.redis.ping()

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"dedup/{name}")

    def process_item(self, item, spider):
        if self.redis is None:
            self.redis = get_redis()

        adapter = ItemAdapter(item)
        source = adapter.get("source") or getattr(spider, "name", "unknown")
        key = dedup_key(source)
        ad_id = str(adapter["ad_id"])

        new_fp = fingerprint(adapter)
        old_fp = self.redis.hget(key, ad_id)

        if old_fp == new_fp or (old_fp and not self.allow_updates):
            self._inc("duplicate")
            raise DropItem(f"[duplicate] {source}:{ad_id}")

        self.redis.hset(key, ad_id, new_fp)
        self._inc("updated" if old_fp else "new")
        return item