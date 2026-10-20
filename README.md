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
- Docker và Docker Compose
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

- **Kafka Hydra (SSL)**: đặt `ca.pem`, `key.pem`, `certificate.pem` vào `chotot/ssl/` (thư mục đã được `.gitignore`, không commit), điền `KAFKA_BOOTSTRAP_SERVERS`.
- **Test local** bằng docker-compose: đặt `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`.
- `ES_HOST`, `END_POINT_API`, `API_KEY_GLOBAL` để trống thì spider bỏ qua bước cập nhật Elasticsearch / trạng thái profile.
- Máy ra Internet qua proxy: `USE_PROXY=true`, `CRAWL_PROXY=http://...`.

## 4. Cài Docker (bỏ qua nếu đã có)

```bash
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY update
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER
```

Cho Docker tải image qua proxy:

```bash
sudo mkdir -p /etc/systemd/system/docker.service.d
printf '[Service]\nEnvironment="HTTP_PROXY=%s"\nEnvironment="HTTPS_PROXY=%s"\nEnvironment="NO_PROXY=localhost,127.0.0.1"\n' $PROXY $PROXY \
  | sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf
printf '{\n  "registry-mirrors": ["https://mirror.gcr.io"],\n  "max-concurrent-downloads": 1,\n  "max-download-attempts": 10\n}\n' \
  | sudo tee /etc/docker/daemon.json
sudo systemctl daemon-reload && sudo systemctl restart docker
```

**Đăng xuất rồi đăng nhập lại Ubuntu** để nhận quyền docker, sau đó kiểm tra:

```bash
docker run --rm hello-world
```

## 5. Chạy Redis, Kafka

Kafka chạy bằng user uid 1000, cần cấp quyền cho volume trước lần chạy đầu:

```bash
docker volume create chotot_kafka_data
docker run --rm -v chotot_kafka_data:/data redis:7-alpine chown -R 1000:1000 /data
docker compose up -d
docker compose ps
```

Cả 3 container `chotot_redis`, `chotot_kafka`, `chotot_kafka_ui` phải ở trạng thái `Up`.

Kafka UI: http://localhost:8080. Topic `chotot.ads` tự tạo khi có message đầu tiên, hoặc tạo tay trên Kafka UI (3 partitions, replication factor 1).

## 6. Chạy crawler

Trên hệ thống Hydra/Crawlab, lệnh chạy chỉ là:

```bash
scrapy crawl chotot
```

Tham số tùy chọn giống spider mẫu: `-a proxy=... -a topic=... -a check_lag=1`.

Test local: đẩy profile giả vào topic input rồi chạy spider:

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
chotot/utils/                    kafka_producer (SSL), hydra, redis_client, normalizer, config_loader
chotot/extensions.py             ghi log ra file và thống kê
config/chotot/_common.yaml       settings Scrapy riêng của spider (delay, concurrency)
scripts/seed_profiles.py         đẩy profile test vào Kafka
```

## Lỗi thường gặp

| Lỗi | Cách xử lý |
|---|---|
| `scrapy: command not found` | Chưa kích hoạt venv: `source venv/bin/activate` |
| `Total source: 0` | Topic input chưa có profile, hoặc group_id đã đọc hết message |
| Lỗi SSL khi kết nối Kafka | Kiểm tra 3 file trong `chotot/ssl/` và `KAFKA_SECURITY_PROTOCOL` |
| `permission denied ... docker.sock` | Chưa đăng xuất/đăng nhập lại sau khi thêm vào nhóm docker |

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
