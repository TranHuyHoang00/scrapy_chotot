from datetime import datetime

from itemadapter import ItemAdapter

from chotot.utils.hydra import get_ad

from chotot.utils.normalizer import (
    VN_TZ,
    clean_multiline,
    clean_text,
    clean_url,
    extract_id,
    parse_posted_time,
    parse_price,
    slugify_key,
    split_location,
    to_iso_utc,
)

TEXT_FIELDS = ("title", "category_name", "city", "district", "price_text")


def normalize_time(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return parse_posted_time(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return to_iso_utc(dt)


def normalize_specs(specs):
    if not isinstance(specs, dict):
        return {}
    result = {}
    for key, value in specs.items():
        new_key = slugify_key(key)
        new_value = clean_text(value) if isinstance(value, str) else value
        if new_key and new_value not in (None, ""):
            result[new_key] = new_value
    return result


def normalize_images(urls):
    if not urls:
        return []
    if isinstance(urls, str):
        urls = [urls]
    seen = []
    for url in urls:
        url = clean_text(url)
        if url and url.startswith("http") and url not in seen:
            seen.append(url)
    return seen


class NormalizePipeline:
    def process_item(self, item, spider=None):
        adapter = ItemAdapter(get_ad(item))

        for field in TEXT_FIELDS:
            if adapter.get(field) is not None:
                adapter[field] = clean_text(adapter[field])

        if adapter.get("description") is not None:
            adapter["description"] = clean_multiline(adapter["description"])

        if adapter.get("url"):
            adapter["url"] = clean_url(adapter["url"])

        if not adapter.get("ad_id") and adapter.get("url"):
            adapter["ad_id"] = extract_id(adapter["url"])
        if adapter.get("ad_id") is not None:
            adapter["ad_id"] = str(adapter["ad_id"]).strip()

        price = adapter.get("price")
        if price is None and adapter.get("price_text"):
            adapter["price"] = parse_price(adapter["price_text"])
        elif isinstance(price, str):
            adapter["price"] = parse_price(price)

        city = adapter.get("city")
        if city and "," in city and not adapter.get("district"):
            adapter["district"], adapter["city"] = split_location(city)

        if adapter.get("posted_at") is not None:
            adapter["posted_at"] = normalize_time(adapter["posted_at"])

        if adapter.get("specs") is not None:
            adapter["specs"] = normalize_specs(adapter["specs"])

        if adapter.get("image_urls") is not None:
            adapter["image_urls"] = normalize_images(adapter["image_urls"])

        return item