from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

DEFAULT_REQUIRED_FIELDS = ("ad_id", "url", "vertical", "category", "title", "price")
DEFAULT_MIN_TITLE_LENGTH = 3


class ValidationPipeline:
    def __init__(self, required_fields, min_title_length, stats=None):
        self.required_fields = tuple(required_fields)
        self.min_title_length = min_title_length
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            required_fields=settings.getlist("VALIDATION_REQUIRED_FIELDS") or DEFAULT_REQUIRED_FIELDS,
            min_title_length=settings.getint("VALIDATION_MIN_TITLE_LENGTH", DEFAULT_MIN_TITLE_LENGTH),
            stats=crawler.stats,
        )

    def _drop(self, reason, adapter):
        if self.stats:
            self.stats.inc_value(f"validation/dropped/{reason}")
            self.stats.inc_value("validation/dropped")
        raise DropItem(f"[{reason}] {adapter.get('url')}")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        missing = [f for f in self.required_fields if adapter.get(f) in (None, "", [], {})]
        if missing:
            self._drop(f"missing_{missing[0]}", adapter)

        if not str(adapter["url"]).startswith(("http://", "https://")):
            self._drop("invalid_url", adapter)

        if len(adapter["title"]) < self.min_title_length:
            self._drop("short_title", adapter)

        price = adapter["price"]
        if not isinstance(price, int) or isinstance(price, bool) or price <= 0:
            self._drop("invalid_price", adapter)

        if self.stats:
            self.stats.inc_value("validation/valid")
        return item