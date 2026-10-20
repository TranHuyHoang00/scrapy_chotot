from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
COMMON_FILE = "_common.yaml"


class ConfigError(Exception):
    pass

def _read_yaml(path):
    if not path.is_file():
        raise ConfigError(f"Không tìm thấy file cấu hình: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"File {path} phải là dạng key: value")
    return data


def _require(data, keys, where):
    missing = [k for k in keys if not data.get(k)]
    if missing:
        raise ConfigError(f"{where} thiếu trường bắt buộc: {', '.join(missing)}")


def source_dir(spider_name):
    path = CONFIG_DIR / spider_name
    if not path.is_dir():
        raise ConfigError(f"Không có thư mục cấu hình: {path}")
    return path


def load_common(spider_name):
    path = source_dir(spider_name) / COMMON_FILE
    data = _read_yaml(path)
    _require(data, ["source", "base_url"], str(path))
    return data


def load_spider_settings(spider_name):
    return load_common(spider_name).get("settings", {}) or {}