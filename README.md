# scrapy_chotot

Spider Scrapy thu thập tin đăng của hệ Chợ Tốt: **chotot.com, xe.chotot.com, nhatot.com, vieclamtot.com**. Spider viết theo quy ước **Hydra** (giống spider mẫu `scrapy_opensanctions`) để chạy trên Crawlab.

```
Kafka input (profile) ─▶ Spider ─▶ Pipeline ─▶ Kafka output (tin đăng)
                          │         normalize → validation → chống trùng (Redis) → gửi Kafka
                          ├─ cập nhật crawled_time (Elasticsearch)
                          ├─ cập nhật source_status (API profile)
                          └─ log [HYDRA-MONITOR] ─▶ Kafka (hydra-spider-log)
```

**Mục lục**

1. [Yêu cầu](#1-yêu-cầu)
2. [Cài đặt](#2-cài-đặt)
3. [Cấu hình `.env`](#3-cấu-hình-env)
4. [Redis (chống trùng)](#4-redis-chống-trùng)
5. [Chạy spider](#5-chạy-spider)
6. [Xem kết quả và log](#6-xem-kết-quả-và-log)
7. [Làm mới Redis / crawl lại từ đầu](#7-làm-mới-redis--crawl-lại-từ-đầu)
8. [Cập nhật code](#8-cập-nhật-code)
9. [Lỗi thường gặp](#9-lỗi-thường-gặp)
10. [Cách hoạt động](#10-cách-hoạt-động)
11. [Cấu trúc thư mục](#11-cấu-trúc-thư-mục)
12. [Còn cần xác nhận với team](#12-còn-cần-xác-nhận-với-team)

---

## 1. Yêu cầu

- Ubuntu/Linux, **Python 3.12+**, Git
- **Docker**: chỉ dùng để chạy Redis local, khi chưa có Redis dev của team
- **Kafka**: Kafka dev của team (gửi qua API HTTP) khi test, Kafka Hydra (broker) khi chạy thật
- **Proxy**, nếu máy phải đi qua proxy mới ra được Internet (ví dụ `http://172.20.148.40:3128`)

Nếu máy cần proxy, đặt biến này để dùng cho các lệnh cài đặt bên dưới:

```bash
export PROXY=http://172.20.148.40:3128
```

Máy không cần proxy thì bỏ các phần `-c http.proxy=$PROXY`, `-x $PROXY`, `--proxy $PROXY` trong các lệnh.

---

## 2. Cài đặt

### 2.1. Clone dự án

```bash
git -c http.proxy=$PROXY clone https://github.com/TranHuyHoang00/scrapy_chotot.git
cd scrapy_chotot
git config http.proxy $PROXY
```

### 2.2. Tạo môi trường Python (venv)

```bash
python3 -m venv --without-pip venv
source venv/bin/activate
curl -x $PROXY -sS --retry 5 https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py --proxy $PROXY
pip install --proxy $PROXY --retries 10 -r requirements.txt
rm get-pip.py
scrapy version
```

Mỗi lần mở terminal mới, cần kích hoạt lại venv:

```bash
cd scrapy_chotot
source venv/bin/activate
```

### 2.3. Cài Docker (bỏ qua nếu đã có, hoặc đã có Redis dev)

```bash
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY update
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER
```

Để Docker tải image qua proxy:

```bash
sudo mkdir -p /etc/systemd/system/docker.service.d
printf '[Service]\nEnvironment="HTTP_PROXY=%s"\nEnvironment="HTTPS_PROXY=%s"\nEnvironment="NO_PROXY=localhost,127.0.0.1"\n' $PROXY $PROXY \
  | sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf
sudo systemctl daemon-reload && sudo systemctl restart docker
```

**Đăng xuất rồi đăng nhập lại Ubuntu** để nhận quyền docker, sau đó kiểm tra:

```bash
docker run --rm hello-world
```

---

## 3. Cấu hình `.env`

```bash
cp .env.example .env
```

File `.env` phải nằm ở **thư mục gốc dự án** (cùng chỗ với `scrapy.cfg`). File này đã có trong `.gitignore`, **không commit**. Trên Crawlab, khai báo các biến này trong phần Environment của spider thay cho file `.env`.

Biến nào không khai báo trong `.env` sẽ dùng giá trị mặc định trong `chotot/settings.py`. `.env.example` liệt kê đầy đủ các biến.

### 3.1. Cấu hình test trên Kafka dev (dùng hằng ngày)

```ini
# Kafka dev qua API HTTP
KAFKA_MODE=http
KAFKA_HTTP_URL=<URL API publish-message của Kafka dev>
KAFKA_HTTP_API_KEY=<API key>
KAFKA_TOPIC_OUTPUT=hydra-update-account-info
KAFKA_LOGGING_ENABLED=false

# Redis local (docker compose)
REDIS_HOST=localhost
REDIS_PORT=6379

# Proxy, nếu máy cần
USE_PROXY=true
CRAWL_PROXY=http://172.20.148.40:3128
```

### 3.2. Cấu hình chạy thật trên Hydra

```ini
KAFKA_MODE=broker
KAFKA_BOOTSTRAP_SERVERS=<host:port>,<host:port>
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_TOPIC_INPUT=hydra-profile-scheduler-chotot
KAFKA_TOPIC_OUTPUT=hydra-chotot
KAFKA_LOGGING_ENABLED=true

REDIS_HOST=<redis của team>
ES_HOST=<elasticsearch>
END_POINT_API=<api profile>
API_KEY_GLOBAL=<api key>
```

Đặt 3 file chứng chỉ Kafka vào `chotot/ssl/`: `ca.pem`, `key.pem`, `certificate.pem`. Thư mục này đã được `.gitignore`.

### 3.3. Ý nghĩa các biến

**Kafka**

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `KAFKA_MODE` | `broker` | `http`: gửi qua API publish-message (Kafka dev). `broker`: kết nối thẳng Kafka (Hydra) |
| `KAFKA_TOPIC_INPUT` | `hydra-profile-scheduler-chotot` | Topic spider **đọc** profile cần crawl (chỉ dùng ở `broker`) |
| `KAFKA_TOPIC_OUTPUT` | `hydra-chotot` | Topic spider **đẩy** dữ liệu tin đăng |
| `KAFKA_TOPIC_LOG` | `hydra-spider-log` | Topic nhận log `[HYDRA-MONITOR]` |
| `KAFKA_LOGGING_ENABLED` | `true` | Gửi log `[HYDRA-MONITOR]` lên Kafka. Test thì đặt `false` |
| `KAFKA_HTTP_URL`, `KAFKA_HTTP_API_KEY` | | URL và key của API Kafka dev (chỉ dùng ở `http`) |
| `KAFKA_HTTP_BATCH_SIZE` | `50` | Số message gom vào 1 request API |
| `KAFKA_HTTP_TIMEOUT` | `30` | Số giây chờ API phản hồi |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Địa chỉ Kafka broker (chỉ dùng ở `broker`) |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `SSL` hoặc `PLAINTEXT` |
| `KAFKA_GROUP_ID` | `group-chotot` | Consumer group khi đọc topic input |

**Redis, Elasticsearch, API profile**

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` | `localhost`, `6379`, `0`, trống | Redis lưu dữ liệu chống trùng |
| `ES_HOST` | trống | Elasticsearch để ghi `crawled_time`. Trống thì bỏ qua |
| `END_POINT_API`, `API_KEY_GLOBAL` | trống | API cập nhật trạng thái profile (alive, dead). Trống thì bỏ qua |

**Crawl**

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `USE_PROXY` | `false` | Bật hoặc tắt proxy khi crawl web |
| `CRAWL_PROXY` | trống | Địa chỉ proxy, vd `http://172.20.148.40:3128` hoặc `http://user:pass@host:port` |
| `CHOTOT_START_URL` | `https://www.chotot.com/mua-ban` | Trang danh sách bắt đầu crawl |
| `CHOTOT_MAX_PAGES` | `5` | Số trang danh sách tối đa (`/mua-ban`, `?page=2`…). Lưu ý: `0` = chỉ trang 1 |
| `CHOTOT_MAX_ITEMS_PER_PAGE` | `0` | Số tin tối đa mỗi trang, `0` = lấy hết |
| `TARGET_ITEM_COUNT` | `0` | Dừng sau N tin hợp lệ, `0` = không giới hạn |
| `DOWNLOAD_DELAY` | `1.5` | Số giây nghỉ giữa 2 request |
| `CONCURRENT_REQUESTS` | `8` | Số request song song tối đa (tổng) |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | `4` | Số request song song tối đa cho mỗi domain |

Proxy chỉ áp dụng cho request crawl web. Kafka, Redis, Elasticsearch, API profile luôn kết nối trực tiếp.

**Log**

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Mức log: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `MONITOR_ENABLED` | `true` | Ghi log ra file `logs/` và in bảng tổng kết cuối lượt chạy |
| `MONITOR_PROGRESS_EVERY` | `50` | In tiến độ sau mỗi N tin |

---

## 4. Redis (chống trùng)

Spider dùng Redis để nhớ những tin đã gửi, lần chạy sau không gửi lại tin không đổi. **Redis bắt buộc phải chạy trước khi chạy spider.**

Chưa có Redis dev của team thì chạy Redis local bằng Docker. `docker-compose.yml` chỉ chứa Redis.

```bash
docker compose up -d --remove-orphans   # bật Redis (--remove-orphans: dọn container cũ không còn dùng)
docker compose ps                       # chotot_redis phải ở trạng thái Up (healthy)
```

Kiểm tra Redis hoạt động:

```bash
docker exec chotot_redis redis-cli ping     # trả về PONG
```

Tắt Redis:

```bash
docker compose stop        # tắt, giữ dữ liệu
docker compose down        # xóa container, vẫn giữ dữ liệu (volume redis_data)
docker compose down -v     # xóa container VÀ toàn bộ dữ liệu chống trùng
```

Dữ liệu lưu trong volume `redis_data`, tắt máy không mất. Redis tự bật lại khi Docker khởi động (`restart: unless-stopped`).

Khi có Redis dev của team: sửa `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` trong `.env`, không cần chạy Docker nữa.

---

## 5. Chạy spider

Luôn chạy tại **thư mục gốc dự án**, đã kích hoạt venv và Redis đang chạy.

### 5.1. Test trên Kafka dev

Ở `KAFKA_MODE=http`, API chỉ **đẩy** được message, không đọc được topic input, nên spider tự tạo 1 profile test từ `start_url`.

Chạy thử ít tin (khuyên dùng lần đầu):

```bash
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban -s CLOSESPIDER_ITEMCOUNT=10
```

Chạy theo cấu hình trong `.env` (`CHOTOT_MAX_PAGES`, `TARGET_ITEM_COUNT`…):

```bash
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban
```

Bỏ `-a start_url=...` thì spider dùng `CHOTOT_START_URL` trong `.env`.

### 5.2. Chạy trên Hydra / Crawlab

```bash
scrapy crawl chotot
```

Spider đọc profile từ `KAFKA_TOPIC_INPUT` (do Hydra scheduler đẩy vào), crawl rồi đẩy kết quả sang `KAFKA_TOPIC_OUTPUT`.

Muốn tự đẩy 1 profile test vào topic input (chỉ dùng được với `KAFKA_MODE=broker`):

```bash
python scripts/seed_profiles.py --pages 2      # đẩy profile
python scripts/seed_profiles.py --dry-run      # chỉ in profile ra, không đẩy
scrapy crawl chotot -s CLOSESPIDER_ITEMCOUNT=30
```

### 5.3. Tham số dòng lệnh

| Tham số | Ý nghĩa |
|---|---|
| `-a start_url=<url>` | Chạy test với 1 profile tự tạo, không đọc Kafka input |
| `-a proxy=<url>` | Proxy cho lần chạy này, ghi đè `.env` |
| `-a topic=<tên>` | Đọc profile từ topic khác `KAFKA_TOPIC_INPUT` |
| `-a check_lag=1` | Ghi log số message còn tồn trong topic input |
| `-s CLOSESPIDER_ITEMCOUNT=<N>` | Dừng sau N tin hợp lệ (ghi đè `TARGET_ITEM_COUNT`) |
| `-s CHOTOT_MAX_PAGES=<N>` | Số trang danh sách cho lần chạy này |
| `-s LOG_LEVEL=DEBUG` | Xem log chi tiết hơn |
| `-o logs/test.json` | Xuất thêm các item ra file JSON để xem nhanh (không thay cho Kafka) |

Dừng spider đang chạy: bấm `Ctrl + C` **một lần** và chờ spider gửi nốt message còn lại rồi tự thoát. Bấm 2 lần sẽ dừng ngay và có thể mất lô message cuối.

---

## 6. Xem kết quả và log

Mỗi lần chạy tạo 1 file log mới: `logs/chotot_<năm tháng ngày>_<giờ phút giây>.log`. Thư mục `logs/` tự tạo nếu chưa có và đã được `.gitignore`.

Các dòng cần xem:

- Đầu log: `Kafka (http) topic output: hydra-update-account-info` cho biết spider đã nhận đúng chế độ và topic.
- Trong lúc chạy:
  - `Trang N: X link tin`: số tin lấy được trên mỗi trang danh sách.
  - `Đã gửi N message lên topic ... qua API`: gửi Kafka thành công.
  - `Gửi Kafka thất bại ...`: gửi lỗi, kèm mã HTTP và nội dung lỗi.
- Cuối log có bảng tổng kết:

```
===== Kết thúc spider 'chotot' sau 85.3s (đủ số item mục tiêu) =====
Request: 25 gửi, 25 phản hồi, 0 thử lại, 0 bị robots.txt chặn
Item: 10 hợp lệ, 2 bị loại
  - Loại vì chotot/missing_title: 2
Chống trùng: 10 mới, 0 cập nhật, 0 trùng
Kafka: 10 gửi, 10 thành công, 0 thất bại
Theo site:
  - chotot: 6
  - xe: 4
```

Xem log mới nhất:

```bash
ls -t logs/*.log | head -1 | xargs tail -n 40
```

Dữ liệu trên Kafka dev xem bằng công cụ của team (topic `KAFKA_TOPIC_OUTPUT`).

Dọn file log cũ hơn 7 ngày:

```bash
find logs -name '*.log' -mtime +7 -delete
```

---

## 7. Làm mới Redis / crawl lại từ đầu

Redis lưu dấu vân tay của mỗi tin đã gửi trong các key `dedup:<site>`:

| Key | Nội dung |
|---|---|
| `dedup:chotot` | tin chotot.com đã gửi: `ad_id` → dấu vân tay |
| `dedup:xe` | tin xe.chotot.com |
| `dedup:nhatot` | tin nhatot.com |
| `dedup:vieclamtot` | tin vieclamtot.com |

Tin có trong Redis và nội dung không đổi sẽ **không được gửi lại**. Muốn gửi lại (vd test lại việc đẩy Kafka) thì xóa dữ liệu này.

**Xem số tin đã lưu:**

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup:*'      # các key đang có
docker exec chotot_redis redis-cli hlen dedup:chotot               # số tin chotot đã gửi
```

**Xóa toàn bộ dữ liệu chống trùng** (lần chạy sau gửi lại mọi tin):

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

**Chỉ xóa 1 site**, vd xe:

```bash
docker exec chotot_redis redis-cli del dedup:xe
```

**Chỉ xóa 1 tin**, vd tin chotot `111210710`:

```bash
docker exec chotot_redis redis-cli hdel dedup:chotot 111210710
```

**Xóa sạch Redis** (Redis local chỉ dùng cho dự án này):

```bash
docker exec chotot_redis redis-cli flushdb
```

Dùng Redis dev của team (không phải Docker) thì thay `docker exec chotot_redis redis-cli` bằng `redis-cli -h <REDIS_HOST> -p <REDIS_PORT> -a <REDIS_PASSWORD>`. **Không dùng `flushdb` trên Redis dùng chung**, vì sẽ xóa dữ liệu của dự án khác.

---

## 8. Cập nhật code

```bash
cd scrapy_chotot
source venv/bin/activate
git pull origin main
pip install --proxy $PROXY -r requirements.txt
docker compose up -d --remove-orphans
```

Sau khi pull, so sánh `.env.example` với `.env` của bạn để thêm biến mới (nếu cần đổi khác mặc định):

```bash
diff <(grep -o '^[A-Z_]*' .env.example | sort) <(grep -o '^[A-Z_]*' .env | sort)
```

Dòng bắt đầu bằng `<` là biến có trong `.env.example` mà `.env` chưa có.

---

## 9. Lỗi thường gặp

| Lỗi | Nguyên nhân / cách xử lý |
|---|---|
| `scrapy: command not found` | Chưa kích hoạt venv: `source venv/bin/activate` |
| `Unknown command: crawl` | Không chạy tại thư mục gốc dự án (nơi có `scrapy.cfg`) |
| `ConnectionError` / `Connection refused` Redis | Redis chưa chạy: `docker compose up -d` |
| `KAFKA_MODE=http cần KAFKA_HTTP_URL và KAFKA_HTTP_API_KEY` | Thiếu 2 biến này trong `.env` |
| `Gửi ... qua API thất bại: HTTP 401/403` | Sai `KAFKA_HTTP_API_KEY` |
| `Gửi ... qua API thất bại: ... timed out` | API Kafka dev không phản hồi; tăng `KAFKA_HTTP_TIMEOUT` hoặc kiểm tra mạng |
| `Total source: 0` | Ở `broker`: topic input chưa có profile, hoặc group đã đọc hết message. Ở test: thêm `-a start_url=...` |
| Lỗi SSL khi kết nối Kafka broker | Kiểm tra 3 file trong `chotot/ssl/` và `KAFKA_SECURITY_PROTOCOL` |
| Không tải được trang nào (timeout, connection refused) | Máy cần proxy: `USE_PROXY=true`, `CRAWL_PROXY=...` |
| `Chống trùng: 0 mới, ... N trùng`, Kafka 0 gửi | Tin đã gửi ở lần trước. Xóa dữ liệu chống trùng (mục 7) |
| `Không có __NEXT_DATA__` / `Không có dữ liệu tin` | Trang bị chặn (vd Cloudflare với nhatot) hoặc tin đã bị ẩn |
| `Chưa có parser cho domain` | Link tin thuộc domain mới, cần thêm parser (mục 10.3) |
| `cannot import name 'classproperty' from 'kafka.util'` | Cài lẫn 2 thư viện Kafka. Chạy: `pip uninstall -y kafka-python kafka-python-ng` rồi `pip install -r requirements.txt` |
| `permission denied ... docker.sock` | Chưa đăng xuất/đăng nhập lại sau khi thêm user vào nhóm docker |

---

## 10. Cách hoạt động

### 10.1. Đầu vào: profile

- **Hydra (`broker`)**: spider đọc topic `KAFKA_TOPIC_INPUT`. Mỗi message là 1 profile, `seed_url`/`url` là trang tổng hợp `https://www.chotot.com/mua-ban`. URL không thuộc chotot.com thì dùng `CHOTOT_START_URL`.
- **Test (`-a start_url` hoặc `KAFKA_MODE=http`)**: spider tự tạo 1 profile test, không đọc Kafka.

Trường profile dùng đến: `profile_id`, `seed_url`/`url`, `name`, `type`, `channel_name`, `tenants`. Có thể thêm (không bắt buộc): `max_pages`, `max_items_per_page`.

### 10.2. Crawl

1. Ghi `crawled_time` của profile vào Elasticsearch (nếu có `ES_HOST`).
2. **Trang danh sách**: `/mua-ban`, `/mua-ban?page=2`… tới `max_pages`. Gặp trang không có tin thì dừng sớm.
   Link tin lấy từ `<li> … <a href=".../<ID>.htm">`. Không dựa vào class CSS vì class sinh tự động, đổi mỗi lần web build lại.
   Link có thể thuộc xe.chotot.com, nhatot.com, vieclamtot.com.
   Trang 1 có tin thì profile được đánh dấu `crawled, alive` (1 lần/lượt chạy); trang 1 lỗi 4xx thì `dead`.
3. **Trang chi tiết**: chọn parser theo domain, đọc JSON trong `<script id="__NEXT_DATA__">` (`props.initialState.adView.adInfo`), tạo `AdItem` theo schema chung.

### 10.3. Parser từng site

| Domain | Parser | Phần riêng |
|---|---|---|
| `chotot.com` | `ChototParser` | gian hàng (`shop_*`), đánh giá, số tin đã bán |
| `xe.chotot.com` | `XeParser` | số km thật từ `mileage_v2` (`mileage` gốc chỉ là mã khoảng), số tin đang đăng |
| `vieclamtot.com` | `VieclamtotParser` | không có `price`; dùng `salary_min/max/type`, tên & logo công ty |
| `nhatot.com` | `NhatotParser` | diện tích, số phòng, tầng dạng số; giá/m² (VNĐ); dự án; tên đường |

Thêm site mới: viết 1 class kế thừa `BaseAdParser` trong `chotot/parsers/sites.py`, thêm domain vào `PARSERS` và `ALLOWED_DOMAINS` trong `chotot/parsers/__init__.py`.

### 10.4. Pipelines

| Thứ tự | Pipeline | Việc làm |
|---|---|---|
| 100 | `normalize` | Làm sạch chữ, bỏ trùng ảnh, bỏ thông số rỗng |
| 200 | `validation` | Loại tin thiếu `ad_id`, `url`, `title` (`VALIDATION_RULES` trong `settings.py`). **Không bắt buộc `price`**: tin cho tặng, thỏa thuận, không ghi giá vẫn giữ với `price = null`; giá sai/âm thì đặt `null` |
| 300 | `redis_dedup` | Loại tin không đổi so với lần gửi trước. Tin chỉ được "đẩy lại" (`updated_at` đổi) cũng không gửi lại; tin đổi giá/nội dung thì gửi bản mới |
| 400 | `kafka_pipeline` | Điền thời gian, `entry_rowkey`, gửi `KAFKA_TOPIC_OUTPUT` |

Gửi Kafka thất bại thì dấu vân tay của tin bị xóa khỏi Redis, lần chạy sau tin được gửi lại.

### 10.5. Gửi Kafka

- `KAFKA_MODE=http`: gom message theo lô `KAFKA_HTTP_BATCH_SIZE`, gọi
  `POST KAFKA_HTTP_URL?apikey=KAFKA_HTTP_API_KEY` với body `{"topic": "...", "message": "[{...}, {...}]"}`. `message` là **chuỗi JSON** của mảng message. Lô cuối gửi khi spider kết thúc.
- `KAFKA_MODE=broker`: gửi thẳng Kafka broker, mỗi tin 1 message, key là `post_id`.

### 10.6. Message gửi Kafka

```json
{
  "url": "https://www.chotot.com/mua-ban-may-tinh-bang-quan-cau-giay-ha-noi/111210710.htm",
  "data": {
    "source": "chotot", "site": "chotot",
    "ad_id": "111210710", "internal_ad_id": "151502709",
    "title": "...", "description": "...",
    "category": "5040", "category_name": "Máy tính bảng", "condition": "Đã sử dụng",
    "price": 2000000, "price_text": "2.000.000 đ",
    "salary_min": null, "salary_max": null, "salary_type": null,
    "location": { "address": "...", "city": "Hà Nội", "city_v3": "TP Hà Nội", "district": "Quận Cầu Giấy",
                  "ward": "Phường Dịch Vọng", "ward_v3": "Phường Cầu Giấy", "lat": 21.03, "lng": 105.79 },
    "posted_at": 1715228380000, "updated_at": 1791591150185, "crawled_at": 1792488483196,
    "images": ["..."], "videos": [],
    "seller": { "account_id": 19441973, "name": "Henry Shop", "is_company": true, "phone_masked": "097567****" },
    "specs": { "tablet_brand": "Apple", "tablet_capacity": "128 GB" },
    "specs_labels": { "tablet_brand": "Hãng", "tablet_capacity": "Dung lượng" }
  },
  "post_id": "111210710",
  "profile_id": "...",
  "entry_rowkey": "md5(post_id) dạng số, 27 ký tự",
  "created_time": 1715228380000,
  "flag_crawl_created_time": 1,
  "updated_time": 1791591150185,
  "inserted_time": 1792488483300,
  "tenant_ids": ["..."], "_tenants": [],
  "docType": "Post", "type": "post",
  "channel_name": "chotot", "owner_name": "...", "author_name": "...", "owner_type": "...",
  "crawler_system": "api-crawler"
}
```

Thời gian là timestamp **mili giây**. `created_time` là lúc đăng tin lần đầu (`flag_crawl_created_time = 1`); không có thì lấy lúc crawl (`= 0`). `updated_time` là lần đẩy tin gần nhất.

### 10.7. Schema `data` (AdItem)

| Trường | Ý nghĩa |
|---|---|
| `source`, `site` | `chotot`; site con: `chotot` / `xe` / `vieclamtot` / `nhatot` |
| `ad_id`, `internal_ad_id` | `list_id` (ID trên URL), `ad_id` nội bộ của Chợ Tốt |
| `url`, `title`, `description`, `category`, `category_name`, `condition` | |
| `price`, `price_text` | giá dạng số, chuỗi hiển thị. Có thể `null`: việc làm, tin cho tặng, không ghi giá |
| `salary_min`, `salary_max`, `salary_type` | chỉ có ở việc làm |
| `location` | `address, city, city_v3, district, ward, ward_v3, region_id, area_id, ward_id, lat, lng` (+ `street` ở nhatot). `*_v3`: tên sau sáp nhập địa giới |
| `posted_at`, `updated_at`, `crawled_at` | ms: đăng lần đầu, lần đẩy tin gần nhất, lúc crawl |
| `images`, `videos` | |
| `seller` | `account_id, account_oid, name, avatar, is_company, phone_masked` + phần riêng từng site |
| `specs`, `specs_labels` | thông số `{id: giá trị}` và `{id: nhãn tiếng Việt}` |

Số điện thoại đầy đủ không có trong `__NEXT_DATA__`, chỉ có dạng che `097567****`.

---

## 11. Cấu trúc thư mục

```
scrapy_chotot/
├── .env.example                  mẫu cấu hình (copy thành .env)
├── docker-compose.yml            Redis local
├── requirements.txt
├── scrapy.cfg
├── logs/                         file log mỗi lần chạy (không commit)
├── scripts/
│   └── seed_profiles.py          đẩy 1 profile test vào topic input (KAFKA_MODE=broker)
└── chotot/
    ├── settings.py               đọc cấu hình từ .env
    ├── items.py                  AdItem: schema chung
    ├── spiders/
    │   ├── base.py               HydraBaseSpider: đọc profile, ES, source_status, log HYDRA-MONITOR
    │   └── chotot_spider.py      trang danh sách → trang chi tiết
    ├── parsers/
    │   ├── __init__.py           chọn parser theo domain
    │   ├── base.py               phần chung đọc __NEXT_DATA__
    │   └── sites.py              ChototParser, XeParser, VieclamtotParser, NhatotParser
    ├── pipelines/                normalize → validation → redis_dedup → kafka_pipeline
    ├── utils/
    │   ├── kafka_sender.py       gửi Kafka: broker | http
    │   ├── kafka_producer.py     kết nối Kafka broker (SSL)
    │   ├── redis_client.py       kết nối Redis
    │   ├── hydra.py              hàm tiện ích Hydra (entry_rowkey, thời gian, profile test)
    │   └── normalizer.py         làm sạch chữ
    ├── kafka_logging.py          gửi log [HYDRA-MONITOR] lên Kafka
    ├── extensions.py             ghi log ra file, bảng tổng kết
    ├── middlewares.py            User-Agent ngẫu nhiên, proxy
    └── ssl/                      chứng chỉ Kafka (không commit)
```

---

## 12. Còn cần xác nhận với team

- Tên topic input/output chính thức và `KAFKA_GROUP_ID` cho chotot.
- Khi chạy thật trên Crawlab: dùng Kafka broker trực tiếp hay qua API HTTP.
- Redis dev cho chống trùng.
- Profile chotot có trỏ tới `https://www.chotot.com/mua-ban` không.
- nhatot.com có chặn Cloudflare với request của spider không.
- Giá trị `crawler_system` cho spider crawl HTML.
