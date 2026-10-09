import scrapy


class AdItem(scrapy.Item):
    ad_id = scrapy.Field()
    url = scrapy.Field()
    source = scrapy.Field()

    vertical = scrapy.Field()
    category = scrapy.Field()
    category_name = scrapy.Field()

    title = scrapy.Field()
    description = scrapy.Field()
    price = scrapy.Field()
    price_text = scrapy.Field()

    specs = scrapy.Field()

    city = scrapy.Field()
    district = scrapy.Field()

    posted_at = scrapy.Field()
    image_urls = scrapy.Field()

    crawled_at = scrapy.Field()