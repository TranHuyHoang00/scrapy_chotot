"""Parser chung cho trang chi tiết tin của hệ Chợ Tốt (đọc JSON __NEXT_DATA__).

chotot, xe.chotot, vieclamtot dùng chung cấu trúc:
    props.initialState.adView.adInfo = {ad, ad_params, parameters, params, headers}
Mỗi site chỉ cần kế thừa BaseAdParser và bổ sung phần riêng trong extra().
"""

from chotot.items import AdItem
from chotot.utils.hydra import now_ms

# chotot.com có cả 2 đường dẫn; xe.chotot, vieclamtot chỉ có đường đầu
AD_INFO_PATHS = (
    ("props", "initialState", "adView", "adInfo"),
    ("props", "pageProps", "initialState", "adView", "adInfo"),
)
CANONICAL_PATHS = (
    ("props", "initialProps", "canonicalUrl"),
    ("props", "canonicalUrl"),
)
LOCATION_PARAMS = {"address", "area", "region", "ward"}
CONDITION_PARAMS = ("condition_ad", "elt_condition")


def dig(data, path):
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def first(data, paths):
    for path in paths:
        value = dig(data, path)
        if value:
            return value
    return None


def to_int(value):
    if value in (None, "", True, False):
        return None
    try:
        return int(str(value).replace(".", "").replace(",", "").strip())
    except ValueError:
        return None


def to_float(value):
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None



class BaseAdParser:
    site = None

    def parse(self, next_data, url):
        """next_data: dict JSON của __NEXT_DATA__. Trả về AdItem hoặc None nếu không có tin."""
        ad_info = first(next_data, AD_INFO_PATHS) or {}
        ad = ad_info.get("ad") or {}
        if not ad:
            return None

        ad_params = ad_info.get("ad_params") or {}
        item = AdItem()
        item["source"] = "chotot"
        item["site"] = self.site
        item["ad_id"] = str(ad.get("list_id") or "") or None
        item["internal_ad_id"] = str(ad["ad_id"]) if ad.get("ad_id") else None
        item["url"] = first(next_data, CANONICAL_PATHS) or url

        item["title"] = ad.get("subject")
        item["description"] = ad.get("body")
        item["category"] = str(ad["category"]) if ad.get("category") else None
        item["category_name"] = ad.get("category_name")
        item["condition"] = self.condition(ad, ad_params)

        item["price"] = to_int(ad.get("price"))
        item["price_text"] = ad.get("price_string")

        item["location"] = self.location(ad, ad_params)
        item["posted_at"] = to_int(ad.get("orig_list_time")) or to_int(ad.get("list_time"))
        item["updated_at"] = to_int(ad.get("list_time"))
        item["crawled_at"] = now_ms()

        item["images"] = list(ad.get("images") or [])
        item["videos"] = self.videos(ad)
        item["seller"] = self.seller(ad)

        specs, labels = self.specs(ad_params)
        item["specs"] = specs
        item["specs_labels"] = labels

        self.extra(item, ad, ad_params)
        return item

    # ------------------------------------------------------------ các phần

    def param_value(self, ad_params, key):
        param = ad_params.get(key)
        return param.get("value") if isinstance(param, dict) else None

    def condition(self, ad, ad_params):
        for key in CONDITION_PARAMS:
            value = self.param_value(ad_params, key)
            if value:
                return value
        return ad.get("condition_ad_name")

    def location(self, ad, ad_params):
        return {
            "address": self.param_value(ad_params, "address") or ad.get("detail_address"),
            "city": ad.get("region_name"),
            "city_v3": ad.get("region_name_v3"),       # tên tỉnh/thành sau sáp nhập
            "district": ad.get("area_name"),
            "ward": ad.get("ward_name"),
            "ward_v3": ad.get("ward_name_v3"),         # tên phường/xã sau sáp nhập
            "region_id": ad.get("region_v2") or ad.get("region"),
            "area_id": ad.get("area_v2") or ad.get("area"),
            "ward_id": ad.get("ward"),
            "lat": to_float(ad.get("latitude")),
            "lng": to_float(ad.get("longitude")),
        }

    def videos(self, ad):
        result = []
        for video in ad.get("videos") or []:
            if isinstance(video, dict):
                result.append({k: video.get(k) for k in ("id", "url", "thumbnail", "gif_url") if video.get(k)})
        return result

    def seller(self, ad):
        return {
            "account_id": ad.get("account_id"),
            "account_oid": ad.get("account_oid"),
            "name": ad.get("full_name") or ad.get("account_name"),
            "avatar": ad.get("avatar"),
            "is_company": bool(ad.get("company_ad")),  # không có trường = người đăng cá nhân
            # __NEXT_DATA__ chỉ có số đã che; số đầy đủ do spider điền sau qua API (utils/phone_api.py)
            "phone": None,
        }

    def specs(self, ad_params):
        """Thông số lấy từ ad_params (giá trị đã là chữ đọc được), bỏ trường vị trí."""
        specs, labels = {}, {}
        for key, param in ad_params.items():
            if key in LOCATION_PARAMS or not isinstance(param, dict):
                continue
            value = param.get("value")
            if value in (None, ""):
                continue
            specs[key] = value
            if param.get("label"):
                labels[key] = param["label"]
        return specs, labels

    def extra(self, item, ad, ad_params):
        """Phần riêng của từng site."""
