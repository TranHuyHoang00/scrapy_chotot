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


def list_groups(spider_name):
    folder = source_dir(spider_name)
    return sorted(
        p.stem for p in folder.glob("*.yaml") if not p.name.startswith("_")
    )


def load_group(spider_name, group):
    path = source_dir(spider_name) / f"{group}.yaml"
    data = _read_yaml(path)
    _require(data, ["vertical", "categories"], str(path))
    data["group"] = group
    return data


def load_categories(spider_name, groups=None):
    common = load_common(spider_name)
    pagination = common.get("pagination", {})
    base_url = common["base_url"].rstrip("/")

    if not groups or groups == ["all"]:
        groups = list_groups(spider_name)

    result = []
    for group in groups:
        group_cfg = load_group(spider_name, group)

        for cat in group_cfg["categories"]:
            where = f"config/{spider_name}/{group}.yaml"
            _require(cat, ["category", "name", "path"], where)

            if not cat.get("enabled", True):
                continue

            max_pages = (
                cat.get("max_pages")
                or group_cfg.get("max_pages")
                or pagination.get("max_pages", 1)
            )
            max_items_per_page = (
                cat.get("max_items_per_page")
                or group_cfg.get("max_items_per_page")
                or pagination.get("max_items_per_page", 0)
            )

            result.append({
                "source": common["source"],
                "group": group,
                "group_name": group_cfg.get("group_name", group),
                "vertical": group_cfg["vertical"],
                "category": cat["category"],
                "category_name": cat["name"],
                "url": base_url + "/" + cat["path"].lstrip("/"),
                "page_param": pagination.get("param", "page"),
                "start_page": pagination.get("start", 1),
                "max_pages": int(max_pages),
                "max_items_per_page": int(max_items_per_page),
            })

    return result


def load_spider_settings(spider_name):
    return load_common(spider_name).get("settings", {}) or {}