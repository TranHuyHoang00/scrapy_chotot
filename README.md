# scrapy_chotot

Hệ thống thu thập dữ liệu bằng Scrapy, dùng Redis để quản lý hàng đợi và chống trùng, đẩy dữ liệu đã chuẩn hóa sang Kafka.

```
Website → Spider (Scrapy) → Pipeline (chuẩn hóa → kiểm tra → chống trùng Redis) → Kafka
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

Sửa `.env` nếu cần. Máy ra Internet qua proxy thì bật proxy cho spider:

```ini
USE_PROXY=true
CRAWL_PROXY=http://172.20.148.40:3128
```

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

## 6. Kiểm tra kết nối

```bash
python -c "
from chotot.utils.redis_client import check_connection as r
from chotot.utils.kafka_producer import check_connection as k, close_producer
print('Redis:', r()); print('Kafka:', k()); close_producer()
"
```

Cả hai phải là `True`.

## 7. Chạy crawler

TARGET_ITEM_COUNT trong .env là số item tối đa sẽ lấy

```bash
python scripts/seed_urls.py chotot dodientu --clear
scrapy crawl chotot
```

Chạy thử số lượng nhỏ:

```bash
scrapy crawl chotot -s CLOSESPIDER_ITEMCOUNT=30 -s MAX_IDLE_TIME=20
```

Log mỗi lần chạy lưu tại `logs/chotot_<thời gian>.log`, cuối log có bảng tổng kết.

## Cấu trúc chính

```
config/<spider>/          cấu hình nguồn: _common.yaml và các nhóm <nhom>.yaml
chotot/spiders/          base.py (dùng chung), <nguon>_spider.py
chotot/pipelines/        normalize → validation → redis_dedup → kafka
chotot/utils/            config_loader, redis_client, kafka_producer, normalizer
chotot/extensions.py     ghi log và thống kê
scripts/seed_urls.py      đẩy URL từ cấu hình vào Redis
```

## Lỗi thường gặp

| Lỗi | Cách xử lý |
|---|---|
| `scrapy: command not found` | Chưa kích hoạt venv: `source venv/bin/activate` |
| `permission denied ... docker.sock` | Chưa đăng xuất/đăng nhập lại sau khi thêm vào nhóm docker |
| Kafka `Restarting`, log có `AccessDeniedException` | Chạy lại lệnh `chown` volume ở bước 5 |
| Tải image bị `unexpected EOF` | Proxy ngắt kết nối, chạy lại `docker compose up -d` |
| `robotstxt/forbidden` trong thống kê | URL bị `robots.txt` của website chặn, cần đổi nguồn URL |


# Luồng hoạt động

## 1. Cấu hình

- `config/chotot/_common.yaml`: cấu hình chung của nguồn (domain, phân trang, tốc độ crawl).
- `config/chotot/<nhom>.yaml`: danh sách danh mục của một nhóm (ví dụ `dodientu.yaml`), bật/tắt bằng `enabled`.
- `.env`: địa chỉ Redis, Kafka, proxy, số item mục tiêu `TARGET_ITEM_COUNT`.

## 2. Đưa URL vào hàng đợi

```bash
python scripts/seed_urls.py chotot dodientu --clear
```

Đọc cấu hình, tạo URL trang danh sách cho các danh mục đang bật, thêm vào Redis `chotot:start_urls`. Mỗi URL kèm thông tin danh mục (`vertical`, `category`, `max_items_per_page`…).

## 3. Spider crawl

```bash
scrapy crawl chotot
```

1. Lấy URL từ hàng đợi Redis.
2. **Trang danh sách**: lấy link tin dạng `/<ID>.htm`, tối đa `max_items_per_page` link mỗi trang.
3. **Trang chi tiết**: đọc JSON `__NEXT_DATA__`, tạo `AdItem` (tiêu đề, giá, vị trí, thông số, ảnh…).

Mọi request đi qua middleware: đổi User-Agent ngẫu nhiên, gắn proxy nếu `USE_PROXY=true`, tôn trọng `robots.txt`.

## 4. Pipelines xử lý item

| Thứ tự | Pipeline | Việc làm |
|---|---|---|
| 100 | `normalize` | Làm sạch chữ, đổi giá sang số, thời gian sang ISO UTC, key thông số không dấu |
| 200 | `validation` | Loại item thiếu trường bắt buộc hoặc giá không hợp lệ |
| 300 | `redis_dedup` | Loại item trùng nội dung đã gửi (lưu dấu vân tay trong Redis) |
| 400 | `kafka_pipeline` | Đóng gói message chuẩn, gửi Kafka |

## 5. Message gửi Kafka

```json
{
  "schema_version": "1.0",
  "event_id": "uuid",
  "event_type": "ad.upsert",
  "event_time": "2026-10-09T08:00:00+00:00",
  "source": "chotot",
  "data": { "ad_id": "...", "title": "...", "price": 8290000, "specs": {}, "...": "đủ 16 trường AdItem" }
}
```

- Key message: `chotot:<ad_id>`.
- `event_type: ad.upsert`: phía nhận dùng `ad_id` làm khóa thêm mới hoặc ghi đè.

## 6. Chống trùng 2 lớp

| Lớp | Lưu ở | Tác dụng |
|---|---|---|
| Bộ lọc URL | `chotot:dupefilter` | Không tải lại trang chi tiết đã crawl |
| Dấu vân tay nội dung | `dedup:chotot` | Không gửi lại tin không đổi; tin đổi giá/nội dung được gửi bản mới |

Gửi Kafka thất bại thì dấu vân tay bị xóa, lần crawl sau tin được gửi lại.

## 7. Khi nào spider dừng

- Đủ `TARGET_ITEM_COUNT` item hợp lệ, hoặc
- Hàng đợi trống quá `MAX_IDLE_TIME` giây (mặc định 60).

Dừng giữa chừng (`Ctrl + C`) thì lần sau chạy `scrapy crawl chotot` sẽ làm tiếp nhờ hàng đợi lưu trong Redis.

## 8. Theo dõi

- Log mỗi lần chạy: `logs/chotot_<thời gian>.log`, cuối log có bảng tổng kết (request, item hợp lệ/bị loại, chống trùng, Kafka, theo danh mục).
- Kafka UI: http://localhost:8080.

## Các key Redis

| Key | Nội dung |
|---|---|
| `chotot:start_urls` | URL trang danh sách chờ crawl |
| `chotot:requests` | Request đang chờ, để chạy tiếp khi bị dừng |
| `chotot:dupefilter` | URL trang chi tiết đã crawl |
| `dedup:chotot` | `ad_id` → dấu vân tay nội dung đã gửi Kafka |