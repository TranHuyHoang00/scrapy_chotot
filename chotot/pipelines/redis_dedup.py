import hashlib
import json

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from chotot.utils.hydra import get_ad
from chotot.utils.redis_client import get_redis

# Bỏ qua khi so sánh: thời điểm crawl, và updated_at (tin chỉ được "đẩy lại" thì không gửi lại)
IGNORED_FIELDS = ("crawled_at", "updated_at")


def dedup_key(site):
    return f"dedup:{site}"


def fingerprint(adapter):
    data = {k: v for k, v in adapter.asdict().items() if k not in IGNORED_FIELDS}
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def unmark(site, ad_id):
    get_redis().hdel(dedup_key(site), str(ad_id))


class RedisDedupPipeline:
    def __init__(self, allow_updates=True, stats=None, crawler=None):
        self.allow_updates = allow_updates
        self.stats = stats
        self.crawler = crawler
        self.redis = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            allow_updates=crawler.settings.getbool("DEDUP_ALLOW_UPDATES", True),
            stats=crawler.stats,
            crawler=crawler,
        )

    def open_spider(self, spider=None):
        self.redis = get_redis()
        self.redis.ping()

    def _spider_name(self):
        spider = getattr(self.crawler, "spider", None)
        return getattr(spider, "name", "unknown")

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"dedup/{name}")

    def process_item(self, item, spider=None):
        if self.redis is None:
            self.redis = get_redis()

        adapter = ItemAdapter(get_ad(item))
        site = adapter.get("site") or self._spider_name()
        key = dedup_key(site)
        ad_id = str(adapter["ad_id"])

        new_fp = fingerprint(adapter)
        old_fp = self.redis.hget(key, ad_id)

        if old_fp == new_fp or (old_fp and not self.allow_updates):
            self._inc("duplicate")
            raise DropItem(f"[duplicate] {site}:{ad_id}")

        self.redis.hset(key, ad_id, new_fp)
        self._inc("updated" if old_fp else "new")
        return item