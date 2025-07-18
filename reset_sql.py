import pymysql

conn = pymysql.connect(
    host='localhost',
    user='root',
    password='!!!!!!!!!!!!비번',
    database='stock_data',
    charset='utf8mb4'
)
with conn.cursor() as cur:
    cur.execute("TRUNCATE TABLE stock_snapshot;")
conn.commit()
conn.close()
print("🧹 테이블 초기화 완료")
