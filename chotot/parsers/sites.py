"""Adapter riêng cho từng site. Chỉ bổ sung những gì phần chung không có."""

from chotot.parsers.base import BaseAdParser, to_int


class ChototParser(BaseAdParser):
    """chotot.com: đồ điện tử, đồ gia dụng... Có gian hàng (shop) và đánh giá người bán."""

    site = "chotot"

    def extra(self, item, ad, ad_params):
        seller = item["seller"]
        shop = ad.get("shop") or {}
        if shop:
            seller["shop_name"] = shop.get("name")
            seller["shop_alias"] = shop.get("alias") or ad.get("shop_alias")
            seller["shop_address"] = shop.get("address")
        seller["rating"] = ad.get("average_rating_for_seller") or ad.get("average_rating")
        seller["total_rating"] = ad.get("total_rating_for_seller") or ad.get("total_rating")
        seller["sold_ads"] = ad.get("sold_ads")


class XeParser(BaseAdParser):
    """xe.chotot.com: ô tô, xe máy, xe tải..."""

    site = "xe"

    def extra(self, item, ad, ad_params):
        specs = item["specs"]
        # ad.mileage / ad_params.mileage là MÃ khoảng (vd 3); số km thật nằm ở mileage_v2 (vd 12000)
        mileage = to_int(ad.get("mileage_v2")) or to_int(specs.get("mileage_v2"))
        specs.pop("mileage_v2", None)
        item["specs_labels"].pop("mileage_v2", None)
        if mileage is not None:
            specs["mileage"] = mileage
            item["specs_labels"]["mileage"] = "Số Km đã đi"
        else:
            specs.pop("mileage", None)

        seller_info = ad.get("seller_info") or {}
        item["seller"]["live_ads"] = seller_info.get("live_ads")


class VieclamtotParser(BaseAdParser):
    """vieclamtot.com: tin tuyển dụng. Không có price, dùng lương min/max."""

    site = "vieclamtot"

    def extra(self, item, ad, ad_params):
        item["salary_min"] = to_int(ad.get("min_salary"))
        item["salary_max"] = to_int(ad.get("max_salary"))
        item["salary_type"] = self.param_value(ad_params, "salary_type")

        seller = item["seller"]
        seller["company_name"] = ad.get("company_name") or self.param_value(ad_params, "company_name")
        seller["company_logo"] = ad.get("company_logo")
        item["specs"]["candidate_applied"] = ad.get("candidate_applied")
        item["specs_labels"]["candidate_applied"] = "Số ứng viên đã ứng tuyển"


class NhatotParser(BaseAdParser):
    """nhatot.com: bất động sản.
    TODO: chưa có dữ liệu mẫu (bị Cloudflare chặn), tạm dùng phần chung."""

    site = "nhatot"
