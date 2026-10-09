import json
from datetime import datetime, timezone

import scrapy
from scrapy_redis.spiders import RedisSpider

from hb_1014.items import AdItem
from hb_1014.utils.config_loader import load_spider_settings
from hb_1014.utils.normalizer import to_iso_utc

CATEGORY_FIELDS = ("vertical", "category", "category_name")


class BaseSpider(RedisSpider):
    name = None

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        if cls.name:
            settings.setdict(load_spider_settings(cls.name), priority="spider")

    def make_request_from_data(self, data):
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        payload = json.loads(data)

        return scrapy.Request(
            url=payload["url"],
            callback=self.parse,
            meta={"cfg": payload.get("meta", {})},
            dont_filter=True,
        )
    def limit_links(self, response, links):
        limit = int(response.meta.get("cfg", {}).get("max_items_per_page") or 0)
        if limit and len(links) > limit:
            self.logger.info(
                "Trang %s: lấy %d/%d link theo max_items_per_page",
                response.meta.get("cfg", {}).get("page"), limit, len(links),
            )
            return links[:limit]
        return links
        
    def follow_detail(self, response, url):
        return response.follow(
            url,
            callback=self.parse_detail,
            meta={"cfg": response.meta.get("cfg", {})},
        )

    def new_item(self, response):
        cfg = response.meta.get("cfg", {})
        item = AdItem()
        item["source"] = cfg.get("source", self.name)
        item["url"] = response.url
        item["crawled_at"] = to_iso_utc(datetime.now(timezone.utc))
        for field in CATEGORY_FIELDS:
            if cfg.get(field):
                item[field] = cfg[field]
        return item


    def parse(self, response):
        raise NotImplementedError(f"{self.name}: chưa viết parse()")

    def parse_detail(self, response):
        raise NotImplementedError(f"{self.name}: chưa viết parse_detail()")