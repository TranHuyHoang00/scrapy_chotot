import scrapy


class AdItem(scrapy.Item):

    # ===== Định danh =====
    ad_id = scrapy.Field()          # ID tin lấy từ URL, khóa chống trùng
    url = scrapy.Field()            # link chi tiết, đã bỏ phần #px=...
    source = scrapy.Field()         # cố định "chotot"

    # ===== Danh mục =====
    category = scrapy.Field()       # mã danh mục, vd: "mua-ban-dien-thoai"
    category_name = scrapy.Field()  # tên hiển thị, vd: "Điện thoại"

    # ===== Nội dung tin =====
    title = scrapy.Field()
    description = scrapy.Field()
    price = scrapy.Field()          # giá dạng số (VND)
    price_text = scrapy.Field()     # giá dạng chữ gốc

    # ===== Thông số chung =====
    brand = scrapy.Field()          # hãng
    model = scrapy.Field()          # dòng máy
    condition = scrapy.Field()      # tình trạng

    # ===== Thông số riêng theo danh mục =====
    specs = scrapy.Field()          # dict, vd: {"storage": "128GB", "ram": "8GB"}

    # ===== Vị trí =====
    city = scrapy.Field()
    district = scrapy.Field()

    # ===== Thời gian và hình ảnh =====
    posted_at = scrapy.Field()
    image_urls = scrapy.Field()

    # ===== Thông tin thu thập =====
    crawled_at = scrapy.Field()