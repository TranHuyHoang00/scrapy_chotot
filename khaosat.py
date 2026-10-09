import json
import re
import sys

from parsel import Selector


def find_dicts(obj, keys, path="root"):
    if isinstance(obj, dict):
        if all(k in obj for k in keys):
            yield path, obj
        for k, v in obj.items():
            yield from find_dicts(v, keys, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from find_dicts(v, keys, f"{path}[{i}]")


def short(value, n=80):
    text = json.dumps(value, ensure_ascii=False)
    return text if len(text) <= n else text[:n] + "..."


print("===== TRANG DANH SACH =====")
sel = Selector(text=open("danhsach.html", encoding="utf-8").read())
links = sel.css("a::attr(href)").getall()
ad_links = sorted({re.sub(r"#.*$", "", l) for l in links if re.search(r"/\d+\.htm", l)})
print("So link tin (da bo trung):", len(ad_links))
for l in ad_links[:5]:
    print("  ", l)

print("\n===== TRANG CHI TIET =====")
sel = Selector(text=open("chitiet.html", encoding="utf-8").read())
raw = sel.css("script#__NEXT_DATA__::text").get()
if not raw:
    print("Khong co __NEXT_DATA__")
    sys.exit()

data = json.loads(raw)
found = list(find_dicts(data, ("list_id", "subject")))
print("So khoi du lieu tin tim thay:", len(found))
if found:
    path, ad = found[0]
    print("Duong dan:", path)
    for k, v in ad.items():
        print(f"  {k}: {short(v)}")

params = list(find_dicts(data, ("id", "label", "value")))
print("\nSo thong so (label/value):", len(params))
for path, p in params[:15]:
    print(f"  {p.get('label')}: {p.get('value')}   <- {path}")