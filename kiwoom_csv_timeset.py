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
    'password': '5737',
    'database': 'stock_data',
    'charset': 'utf8mb4'
}

# 안전한 숫자 변환
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

# 종목 정보 조회
def fetch_stock_info(token, shcode):
    url = "https://api.kiwoom.com/api/dostk/mrkcond"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "api-id": "ka10007"
    }
    payload = {"stk_cd": shcode}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# DB 저장
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

# CSV에서 종목코드 로드
def load_stock_list(file):
    stock_list = []
    with open(file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stock_list.append(row["종목코드"])
    return stock_list

# 실패 로깅
def log_failed_code(code):
    with open("failed_codes.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {code}\n")

# 작업 실행 함수
def run_job(file):
    token = get_token()
    stock_codes = load_stock_list(file)
    success_count = 0
    for code in stock_codes:
        info = fetch_stock_info(token, code)
        if info:
            save_snapshot(info)
            success_count += 1
        else:
            print(f"❌ 실패: {code}")
            log_failed_code(code)
        time.sleep(0.2)
    print(f"📊 총 저장된 종목 수: {success_count} / {len(stock_codes)}")

# 예약된 시간까지 대기
def wait_until(target_time_str):
    print(f"⏳ 실행 대기중... 대상 시간: {target_time_str}")
    while True:
        now = datetime.now().strftime("%H:%M")
        if now == target_time_str:
            print(f"🚀 실행 시작: {now}")
            break
        time.sleep(10)

# 실행 시간 & 파일 매핑
SCHEDULES = {
    "15:35": "코스피_전종목.csv",
    "15:35": "코스닥_전종목.csv",
    "15:35": "코넥스_전종목.csv",
    "16:46": "넥스트_전종목.csv",
    "20:01": "넥스트_전종목.csv"
}

# 실행부
if __name__ == '__main__':
    already_run = set()

    while True:
        now = datetime.now().strftime("%H:%M")
        for run_time, file in SCHEDULES.items():
            if run_time == now and (run_time, file) not in already_run:
                run_job(file)
                already_run.add((run_time, file))
        time.sleep(10)
