# scrapy_chotot

Spider Scrapy thu thập tin đăng chotot.com, viết theo quy ước **Hydra** (giống spider mẫu `scrapy_opensanctions`) để chạy được trên Crawlab:

```
Kafka (hydra-profile-scheduler-chotot) → Spider → Pipeline (chuẩn hóa → kiểm tra → chống trùng Redis) → Kafka (hydra-chotot)
                                            ├─ cập nhật crawled_time (Elasticsearch)
                                            ├─ cập nhật source_status (API profile)
                                            └─ log [HYDRA-MONITOR] → Kafka (hydra-spider-log)
```

## Yêu cầu

- Ubuntu/Linux, Python 3.12+, Git
- Kafka dev của team
- Docker (chỉ để chạy Redis local, khi chưa có Redis dev)
- Proxy (ví dụ dùng `http://172.20.148.40:3128`, thay bằng proxy của bạn)

Đặt biến proxy để dùng cho các lệnh bên dưới:

```bash
export PROXY=http://172.20.148.40:3128
```

## 1. Clone dự án

```bash
git -c http.proxy=$PROXY clone https://github.com/TranHuyHoang00/scrapy_chotot.git
cd scrapy_chotot
git config http.proxy $PROXY
```

## 2. Tạo môi trường Python

```bash
python3 -m venv --without-pip venv
source venv/bin/activate
curl -x $PROXY -sS --retry 5 https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py --proxy $PROXY
pip install --proxy $PROXY --retries 10 -r requirements.txt
rm get-pip.py
scrapy version
```

## 3. Cấu hình

```bash
cp .env.example .env
```

Toàn bộ địa chỉ máy chủ, key, mật khẩu đọc từ biến môi trường, **không ghi trong code**. Trên Crawlab, khai báo các biến trong `.env.example` ở phần Environment của spider.

- **Kafka**: điền `KAFKA_BOOTSTRAP_SERVERS` của Kafka dev (khi test) hoặc Kafka Hydra (khi chạy thật). Kafka dùng SSL thì đặt `ca.pem`, `key.pem`, `certificate.pem` vào `chotot/ssl/` (đã `.gitignore`, không commit); không SSL thì đặt `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`.
- **Redis** (chống trùng tin): chưa có Redis dev thì chạy Redis local (mục 4), giữ `REDIS_HOST=localhost`, `REDIS_PORT=6379`.
- `ES_HOST`, `END_POINT_API`, `API_KEY_GLOBAL` để trống thì spider bỏ qua bước cập nhật Elasticsearch / trạng thái profile.
- Máy ra Internet qua proxy: `USE_PROXY=true`, `CRAWL_PROXY=http://...`.
- Số trang, số tin, tốc độ crawl (`CHOTOT_MAX_PAGES`, `DOWNLOAD_DELAY`, `CONCURRENT_REQUESTS`...) chỉ cấu hình trong `.env`.

## 4. Chạy Redis local

Chỉ cần khi chưa có Redis dev. `docker-compose.yml` chỉ chứa Redis.

```bash
docker compose up -d
docker compose ps          # chotot_redis phải ở trạng thái Up (healthy)
```

Dữ liệu chống trùng lưu trong volume `redis_data`, tắt máy không mất. Muốn crawl lại từ đầu (gửi lại mọi tin):

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

Máy cần proxy để tải image: cấu hình proxy cho Docker daemon (`/etc/systemd/system/docker.service.d/http-proxy.conf`).

## 5. Chạy crawler

Trên hệ thống Hydra/Crawlab, lệnh chạy chỉ là:

```bash
scrapy crawl chotot
```

Tham số tùy chọn giống spider mẫu: `-a proxy=... -a topic=... -a check_lag=1`. Thêm `-a start_url=...` để chạy test không cần đọc Kafka input.

### Test trên Kafka dev (qua API publish-message)

Kafka dev là API HTTP, chỉ **đẩy** được message, không đọc được topic input. Đặt trong `.env`:

```ini
KAFKA_MODE=http
KAFKA_HTTP_URL=<URL API publish-message của Kafka dev>
KAFKA_HTTP_API_KEY=<API key>
KAFKA_TOPIC_OUTPUT=hydra-update-account-info   # topic test
KAFKA_LOGGING_ENABLED=false                    # không đẩy log HYDRA-MONITOR lên Kafka dev
```

Chạy (spider tự tạo 1 profile test, không đọc Kafka input):

```bash
docker compose up -d                     # Redis chống trùng
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban -s CLOSESPIDER_ITEMCOUNT=10
```

Message được gom theo lô `KAFKA_HTTP_BATCH_SIZE` (mặc định 50), lô cuối gửi khi spider kết thúc. Body gửi API: `{"topic": "...", "message": "[{...}, {...}]"}` (`message` là chuỗi JSON của mảng item Hydra).

Tin đã gửi sẽ không gửi lại ở lần chạy sau (chống trùng). Muốn gửi lại thì xóa dữ liệu chống trùng trong Redis (mục 4).

### Chạy với Kafka broker (Hydra)

`KAFKA_MODE=broker`, điền `KAFKA_BOOTSTRAP_SERVERS` và chứng chỉ SSL. Spider đọc profile từ `KAFKA_TOPIC_INPUT`. Có thể đẩy 1 profile test vào topic input:

```bash
python scripts/seed_profiles.py --pages 2
scrapy crawl chotot -s CLOSESPIDER_ITEMCOUNT=30
```

Log mỗi lần chạy lưu tại `logs/chotot_<thời gian>.log`, cuối log có bảng tổng kết.

## Cấu trúc chính

```
chotot/spiders/base.py           HydraBaseSpider: đọc profile Kafka, ES, source_status, log HYDRA-MONITOR
chotot/spiders/chotot_spider.py  trang danh sách → trang chi tiết → AdItem
chotot/pipelines/                normalize → validation → redis_dedup → kafka (format Hydra)
chotot/kafka_logging.py          gửi log [HYDRA-MONITOR] sang hydra-spider-log
chotot/utils/                    kafka_sender (broker | http), kafka_producer (SSL), hydra, redis_client, normalizer
chotot/extensions.py             ghi log ra file và thống kê
scripts/seed_profiles.py         đẩy profile test vào topic input (Kafka dev)
```

## Lỗi thường gặp

| Lỗi | Cách xử lý |
|---|---|
| `scrapy: command not found` | Chưa kích hoạt venv: `source venv/bin/activate` |
| `ConnectionError` Redis | Chưa chạy Redis: `docker compose up -d` |
| `Total source: 0` | Topic input chưa có profile, hoặc group_id đã đọc hết message |
| Lỗi SSL khi kết nối Kafka | Kiểm tra 3 file trong `chotot/ssl/` và `KAFKA_SECURITY_PROTOCOL` |

# Luồng hoạt động

## 1. Đầu vào: profile từ Kafka

Spider đọc topic `KAFKA_TOPIC_INPUT` (mặc định `hydra-profile-scheduler-chotot`). Mỗi message là một profile, `seed_url`/`url` là **trang tổng hợp toàn bộ tin** `https://www.chotot.com/mua-ban` (không crawl theo danh mục). URL không thuộc chotot.com thì spider dùng `CHOTOT_START_URL`.

Trường dùng đến: `profile_id`, `seed_url`/`url`, `name`, `type`, `channel_name`, `tenants`. Có thể thêm (không bắt buộc): `max_pages`, `max_items_per_page`.

URL không có `http(s)://` → đánh dấu profile `No http/https` và bỏ qua.

## 2. Crawl

1. Ghi `crawled_time` của profile vào Elasticsearch.
2. **Trang danh sách**: `/mua-ban`, rồi `/mua-ban?page=2`, `?page=3`... tới `max_pages` (mặc định `CHOTOT_MAX_PAGES`); gặp trang không có tin thì dừng.
   Link tin lấy từ thẻ `<li> ... <a href=".../<ID>.htm">`, có thể thuộc domain khác của Chợ Tốt như `nhatot.com` (không dựa vào class vì class sinh tự động).
   Trang đầu có tin → profile `crawled, alive` (gửi 1 lần/lượt chạy). Trang đầu lỗi 4xx → `dead`.
3. **Trang chi tiết**: chọn parser theo domain, đọc JSON `__NEXT_DATA__` (`props.initialState.adView.adInfo`), tạo `AdItem` theo schema chung, bọc thành item Hydra `{"item_data": {...}}`.

| Domain | Parser | Phần riêng |
|---|---|---|
| `chotot.com` | `ChototParser` | gian hàng (`shop_*`), đánh giá, số tin đã bán |
| `xe.chotot.com` | `XeParser` | số km thật lấy từ `mileage_v2` (`mileage` gốc chỉ là mã khoảng) |
| `vieclamtot.com` | `VieclamtotParser` | không có `price`; dùng `salary_min/max/type`, tên & logo công ty |
| `nhatot.com` | `NhatotParser` | diện tích, số phòng, tầng dạng số; giá/m² (VNĐ); dự án; tên đường (`location.street`) |

Code: `chotot/parsers/base.py` (phần chung), `chotot/parsers/sites.py` (adapter), `chotot/parsers/__init__.py` (chọn theo domain).
Thêm site mới: viết 1 class kế thừa `BaseAdParser`, thêm domain vào `PARSERS` và `ALLOWED_DOMAINS`.

## 3. Pipelines

| Thứ tự | Pipeline | Việc làm |
|---|---|---|
| 100 | `normalize` | Làm sạch chữ, bỏ trùng ảnh, bỏ thông số rỗng |
| 200 | `validation` | Loại tin thiếu trường bắt buộc theo site (`VALIDATION_RULES`; việc làm không bắt buộc `price`) hoặc giá không hợp lệ |
| 300 | `redis_dedup` | Loại tin không đổi so với lần gửi trước (dấu vân tay trong Redis `dedup:<site>`); tin chỉ được đẩy lại (`updated_at` đổi) cũng không gửi lại |
| 400 | `kafka_pipeline` | Điền thời gian, `entry_rowkey`, gửi `KAFKA_TOPIC_OUTPUT` |

Gửi Kafka thất bại thì dấu vân tay bị xóa, lần crawl sau tin được gửi lại.

## 4. Message gửi Kafka (`hydra-chotot`)

```json
{
  "url": "https://www.chotot.com/.../123456789.htm",
  "data": { "site": "chotot", "ad_id": "123456789", "title": "...", "price": 8290000, "location": {}, "seller": {}, "specs": {}, "...": "đủ trường AdItem" },
  "post_id": "123456789",
  "profile_id": "...",
  "entry_rowkey": "md5(post_id) dạng số, 27 ký tự",
  "created_time": 1760000000000,
  "flag_crawl_created_time": 1,
  "updated_time": 1760000000000,
  "inserted_time": 1760000000000,
  "tenant_ids": ["..."], "_tenants": [],
  "docType": "Post", "type": "post",
  "channel_name": "chotot", "owner_name": "...", "author_name": "...", "owner_type": "...",
  "crawler_system": "api-crawler"
}
```

Thời gian là timestamp mili giây. `created_time` lấy theo thời điểm đăng tin (`flag_crawl_created_time = 1`); không có thì lấy thời điểm crawl (`= 0`).

### Schema `data` (AdItem)

| Trường | Ý nghĩa |
|---|---|
| `source`, `site` | `chotot`; site con: `chotot` / `xe` / `vieclamtot` / `nhatot` |
| `ad_id`, `internal_ad_id` | `list_id` (ID trên URL), `ad_id` nội bộ |
| `url`, `title`, `description`, `category`, `category_name`, `condition` | |
| `price`, `price_text` | giá số (việc làm = null), chuỗi hiển thị |
| `salary_min`, `salary_max`, `salary_type` | chỉ việc làm |
| `location` | `address, city, city_v3, district, ward, ward_v3, region_id, area_id, ward_id, lat, lng` (`*_v3`: tên sau sáp nhập) |
| `posted_at`, `updated_at`, `crawled_at` | ms: đăng lần đầu (`orig_list_time`), lần đẩy tin gần nhất (`list_time`), lúc crawl |
| `images`, `videos` | |
| `seller` | `account_id, account_oid, name, avatar, is_company, phone_masked` + phần riêng site |
| `specs`, `specs_labels` | thông số `{id: giá trị đọc được}`, `{id: nhãn tiếng Việt}` |

Số điện thoại đầy đủ không có trong `__NEXT_DATA__` (chỉ có dạng che `097567****`).

## 5. Giám sát

- Log có tag `[HYDRA-MONITOR]` được gửi sang `hydra-spider-log` kèm `CRAWLAB_TASK_ID`.
- File log local: `logs/chotot_<thời gian>.log`.

## Còn cần xác nhận với team

- Tên topic input/output và `KAFKA_GROUP_ID` cho chotot.
- Profile chotot có trỏ tới `https://www.chotot.com/mua-ban` như hiện tại không.
- nhatot.com chặn Cloudflare khi mở bằng trình duyệt: cần chạy thử spider xem request có bị chặn không.
- Giá trị `crawler_system` cho spider crawl HTML.
