"""Kết nối Kafka dùng chung để gửi message.

Cấu hình đọc từ .env:
    KAFKA_BOOTSTRAP_SERVERS   vd: localhost:9092 (nhiều server cách nhau dấu phẩy)
    KAFKA_TOPIC               topic mặc định
"""

import json
import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


def kafka_servers():
    raw = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [s.strip() for s in raw.split(",") if s.strip()]


def default_topic():
    return os.getenv("KAFKA_TOPIC", "hb1014.chotot.ads")


def _serialize_value(value):
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")


def _serialize_key(key):
    return str(key).encode("utf-8") if key is not None else None


@lru_cache(maxsize=1)
def get_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_servers(),
        value_serializer=_serialize_value,
        key_serializer=_serialize_key,
        acks="all",             
        retries=5,             
        linger_ms=50,            
        compression_type="gzip",  
        request_timeout_ms=30000,
    )


def send_message(value, key=None, topic=None):
    return get_producer().send(topic or default_topic(), value=value, key=key)


def close_producer():
    if get_producer.cache_info().currsize:
        producer = get_producer()
        producer.flush()
        producer.close()
        get_producer.cache_clear()


def check_connection(topic=None):
    try:
        partitions = get_producer().partitions_for(topic or default_topic())
        return bool(partitions)
    except KafkaError:
        return False