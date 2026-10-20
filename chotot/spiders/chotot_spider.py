import json
from datetime import datetime, timezone

import scrapy
from w3lib.url import add_or_replace_parameter

from chotot.items import AdItem
from chotot.spiders.base import HydraBaseSpider
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


class ChototSpider(HydraBaseSpider):
    """Crawl trang tổng hợp toàn bộ tin: https://www.chotot.com/mua-ban?page=N

    URL lấy từ seed_url/url của profile Hydra; profile không có URL hợp lệ thì dùng CHOTOT_START_URL.
    Link chi tiết có thể thuộc domain khác của Chợ Tốt (nhatot.com, xe.chotot.com...).

    Có thể khai báo thêm trong profile (không bắt buộc):
        max_pages            số trang danh sách tối đa (mặc định CHOTOT_MAX_PAGES)
        max_items_per_page   số tin tối đa mỗi trang, 0 = lấy hết
    """

    name = "chotot"
    allowed_domains = ["chotot.com", "nhatot.com"]

    def _inc(self, name):
        self.crawler.stats.inc_value(f"chotot/{name}")

    # ------------------------------------------------------------ danh sách

    def start_profile(self, profile, url):
        if "chotot.com" not in url:
            url = self.settings.get("CHOTOT_START_URL")
        yield self.list_request(profile, url, 1)

    def list_request(self, profile, base_url, page):
        page_param = self.settings.get("CHOTOT_PAGE_PARAM", "page")
        url = base_url if page == 1 else add_or_replace_parameter(base_url, page_param, str(page))
        return scrapy.Request(
            url,
            callback=self.parse,
            # chỉ trang 1 lỗi mới đánh dấu profile dead
            errback=self.error_back_start_request if page == 1 else None,
            meta=self.request_meta(profile, base_url=base_url, page=page),
            dont_filter=True,
        )

    def _profile_int(self, profile, key, setting, default):
        value = profile.get(key)
        if value in (None, ""):
            return self.settings.getint(setting, default)
        return int(value)

    def extract_links(self, response):
        """Link tin nằm trong <li> của danh sách: <li ...><div ...><a href=".../135011939.htm">.
        Không dùng class (a14axl8t, cwv3xk0...) vì là class sinh tự động, dễ đổi."""
        hrefs = response.css("li a::attr(href)").re(AD_LINK_REGEX)
        links = [response.urljoin(h) for h in hrefs]
        return list(dict.fromkeys(links))

    def parse(self, response):
        profile = response.meta["profile"]
        page = response.meta["page"]
        profile_id = profile.get("profile_id")

        links = self.extract_links(response)
        limit = self._profile_int(profile, "max_items_per_page", "CHOTOT_MAX_ITEMS_PER_PAGE", 0)
        if limit and len(links) > limit:
            links = links[:limit]

        if not links:
            self._inc("list_empty")
            self.monitor("WARNING", f"[{profile_id}] Trang {page} không có link tin, dừng phân trang: {response.url}")
            return

        self._inc("list_pages")
        if page == 1:
            self.update_status_source(profile_id, ["crawled", "alive"], once=True)
        self.logger.info("[%s] Trang %s: %d link tin", profile_id, page, len(links))

        for link in links:
            yield scrapy.Request(
                link,
                callback=self.parse_detail,
                meta=self.request_meta(profile),
            )

        max_pages = self._profile_int(profile, "max_pages", "CHOTOT_MAX_PAGES", 5)
        if page < max_pages:
            yield self.list_request(profile, response.meta["base_url"], page + 1)

    # ------------------------------------------------------------- chi tiết

    def parse_detail(self, response):
        profile = response.meta["profile"]

        raw = response.css("script#__NEXT_DATA__::text").get()
        if not raw:
            self._inc("detail_no_json")
            self.logger.warning("Không có __NEXT_DATA__: %s", response.url)
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
            self.logger.warning("Không có dữ liệu tin: %s", response.url)
            return

        item = AdItem()
        item["source"] = self.name
        item["ad_id"] = str(ad.get("list_id") or extract_id(response.url))
        item["url"] = clean_url(response.url)
        # TODO: phần chi tiết sẽ làm lại sau (tin nhatot.com có thể khác cấu trúc)
        item["category"] = str(ad["category"]) if ad.get("category") else None
        item["category_name"] = ad.get("category_name")
        item["title"] = ad.get("subject")
        item["description"] = ad.get("body")
        item["price"] = ad.get("price")
        item["price_text"] = ad.get("price_string")
        item["city"] = ad.get("region_name_v3") or ad.get("region_name")
        item["district"] = ad.get("area_name")
        item["posted_at"] = ms_to_iso(ad.get("list_time")) or ad.get("date")
        item["image_urls"] = ad.get("images") or []
        item["specs"] = self.extract_specs(ad_info.get("ad_params") or {})
        item["crawled_at"] = to_iso_utc(datetime.now(timezone.utc))

        self._inc("detail_parsed")
        yield self.build_item_data(profile, item["url"], item["ad_id"], item)

    def extract_specs(self, params):
        specs = {}
        for key, param in params.items():
            if key in LOCATION_PARAMS or not isinstance(param, dict):
                continue
            value = param.get("value")
            if value not in (None, ""):
                specs[key] = value
        return specs
