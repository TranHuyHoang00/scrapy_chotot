import json

import scrapy
from w3lib.url import add_or_replace_parameter

from chotot.parsers import ALLOWED_DOMAINS, get_parser
from chotot.spiders.base import HydraBaseSpider

AD_LINK_REGEX = r"^[^#?]*/\d+\.htm"


class ChototSpider(HydraBaseSpider):
    """Crawl trang tổng hợp toàn bộ tin: https://www.chotot.com/mua-ban?page=N

    URL lấy từ seed_url/url của profile Hydra; profile không có URL hợp lệ thì dùng CHOTOT_START_URL.
    Link chi tiết có thể thuộc các site khác của Chợ Tốt (xe.chotot.com, vieclamtot.com, nhatot.com);
    mỗi site có parser riêng trong chotot/parsers/.

    Có thể khai báo thêm trong profile (không bắt buộc):
        max_pages            số trang danh sách tối đa (mặc định CHOTOT_MAX_PAGES)
        max_items_per_page   số tin tối đa mỗi trang, 0 = lấy hết
    """

    name = "chotot"
    allowed_domains = ALLOWED_DOMAINS

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

        parser = get_parser(response.url)
        if parser is None:
            self._inc("detail_unknown_site")
            self.monitor("WARNING", f"Chưa có parser cho domain: {response.url}")
            return

        raw = response.css("script#__NEXT_DATA__::text").get()
        if not raw:
            self._inc(f"{parser.site}/no_json")
            self.monitor("WARNING", f"[{parser.site}] Không có __NEXT_DATA__: {response.url}")
            return

        try:
            next_data = json.loads(raw)
        except ValueError:
            self._inc(f"{parser.site}/bad_json")
            return

        item = parser.parse(next_data, response.url)
        if item is None:
            self._inc(f"{parser.site}/no_ad")
            self.monitor("WARNING", f"[{parser.site}] Không có dữ liệu tin: {response.url}")
            return

        self._inc(f"{parser.site}/parsed")
        yield self.build_item_data(profile, item["url"], item["ad_id"], item)
