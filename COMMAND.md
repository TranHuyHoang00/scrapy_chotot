# Linux - Qua proxy
# Đặt USE_PROXY = True trong .py trước khi chạy

rm -rf venv
python3 -m venv --without-pip venv
source venv/bin/activate
curl -x http://172.20.148.40:3128 -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py --proxy http://172.20.148.40:3128
pip install --proxy http://172.20.148.40:3128 -r requirements.txt

python dichvucong.py
python ca-news-yahoo.py
python scmp.py

# Chạy lại không cần cài

source venv/bin/activate

python dichvucong.py
python ca-news-yahoo.py
python scmp.py

# Windows (PowerShell) - Không qua proxy
# Đặt USE_PROXY = False trong .py trước khi chạy

python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt

python dichvucong.py
python ca-news-yahoo.py
python scmp.py

# Chạy lại không cần cài

venv\Scripts\Activate.ps1

python dichvucong.py
python ca-news-yahoo.py
python scmp.py