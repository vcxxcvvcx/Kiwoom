import requests
import pymysql
import csv
import time
from datetime import datetime
from kiwoom_key import appkey, secretkey

# MySQL 접속 설정
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '비밀번호 네자리 입력',
    'database': 'stock_data',
    'charset': 'utf8mb4'
}

# 안전하게 숫자 변환
def safe_int(value):
    try:
        return int(str(value).replace(',', '').strip())
    except:
        return 0

# 접근토큰 발급
def get_token():
    url = 'https://api.kiwoom.com/oauth2/token'
    payload = {
        'grant_type': 'client_credentials',
        'appkey': appkey,
        'secretkey': secretkey
    }
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    res = requests.post(url, json=payload, headers=headers)
    return res.json().get("token")

# 단일 종목 조회
def fetch_stock_info(token, shcode):
    url = "https://api.kiwoom.com/api/dostk/mrkcond"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "api-id": "ka10007"
    }
    payload = { "stk_cd": shcode }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ 요청 실패: {shcode}, 코드: {response.status_code}")
        return None

# MySQL 저장
def save_snapshot(data):
    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO stock_snapshot (
                    shcode, hname, price, change_pct, amount,
                    listed_shares, total_offer_qty, total_bid_qty, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, (
                    data.get("stk_cd"),
                    data.get("stk_nm"),
                    safe_int(data.get("cur_prc")),
                    float(data.get("flu_rt") or 0),
                    safe_int(data.get("trde_qty")),
                    safe_int(data.get("flo_stkcnt")),
                    safe_int(data.get("tot_sel_req")),
                    safe_int(data.get("tot_buy_req")),
                    datetime.now()
                ))
            conn.commit()
        print(f"✅ 저장 완료: {data.get('stk_nm')}")
    except Exception as e:
        print(f"❌ 저장 실패: {data.get('stk_cd')} - {e}")

# CSV 파일 목록
CSV_FILES = [
    "코스피_전종목.csv",
    "코스닥_전종목.csv",
    "코넥스_전종목.csv"
]

# CSV에서 종목코드 리스트 추출
def load_stock_list():
    stock_list = []
    for file in CSV_FILES:
        with open(file, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stock_list.append(row["종목코드"])
    return stock_list

# 실행부
if __name__ == '__main__':
    token = get_token()
    stock_codes = load_stock_list()

    for code in stock_codes:
        info = fetch_stock_info(token, code)
        if info:
            save_snapshot(info)
        time.sleep(0.2)  # API 호출 간 딜레이
