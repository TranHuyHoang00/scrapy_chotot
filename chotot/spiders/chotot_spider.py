import json
from datetime import datetime, timezone

from chotot.spiders.base import BaseSpider
from chotot.utils.normalizer import clean_url, extract_id, to_iso_utc

AD_LINK_REGEX = r"^[^#?]*/\d+\.htm"
AD_INFO_PATH = ("props", "pageProps", "initialState", "adView", "adInfo")
LOCATION_PARAMS = {"address", "area", "region", "ward"}


def dig(data, path):
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def ms_to_iso(value):
    if not value:
        return None
    try:
        return to_iso_utc(datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc))
    except (TypeError, ValueError, OverflowError):
        return None


class ChototSpider(BaseSpider):
    name = "chotot"
    allowed_domains = ["chotot.com"]

    def _inc(self, name):
        self.crawler.stats.inc_value(f"chotot/{name}")

    def parse(self, response):
        links = list(dict.fromkeys(response.css("a::attr(href)").re(AD_LINK_REGEX)))
        links = self.limit_links(response, links)
        page = response.meta.get("cfg", {}).get("page")

        if not links:
            self._inc("list_empty")
            self.logger.warning("Khong tim thay link tin: %s", response.url)
            return

        self._inc("list_pages")
        self.logger.info("Trang %s: %d link tin", page, len(links))

        for link in links:
            yield self.follow_detail(response, link)

    def parse_detail(self, response):
        raw = response.css("script#__NEXT_DATA__::text").get()
        if not raw:
            self._inc("detail_no_json")
            self.logger.warning("Khong co __NEXT_DATA__: %s", response.url)
            return

        try:
            data = json.loads(raw)
        except ValueError:
            self._inc("detail_bad_json")
            return

        ad_info = dig(data, AD_INFO_PATH) or {}
        ad = ad_info.get("ad") or {}
        if not ad:
            self._inc("detail_no_ad")
            self.logger.warning("Khong co du lieu tin: %s", response.url)
            return

        item = self.new_item(response)
        item["ad_id"] = str(ad.get("list_id") or extract_id(response.url))
        item["url"] = clean_url(response.url)
        item["title"] = ad.get("subject")
        item["description"] = ad.get("body")
        item["price"] = ad.get("price")
        item["price_text"] = ad.get("price_string")
        item["city"] = ad.get("region_name_v3") or ad.get("region_name")
        item["district"] = ad.get("area_name")
        item["posted_at"] = ms_to_iso(ad.get("list_time")) or ad.get("date")
        item["image_urls"] = ad.get("images") or []
        item["specs"] = self.extract_specs(ad_info.get("ad_params") or {})
        if ad.get("category_name"):
            item["category_name"] = ad["category_name"]

        self._inc("detail_parsed")
        yield item

    def extract_specs(self, params):
        specs = {}
        for key, param in params.items():
            if key in LOCATION_PARAMS or not isinstance(param, dict):
                continue
            value = param.get("value")
            if value not in (None, ""):
                specs[key] = value
        return specs