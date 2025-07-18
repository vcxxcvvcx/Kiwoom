import requests
import json
from kiwoom_key import appkey, secretkey

def get_token():
    url = "https://api.kiwoom.com/oauth2/token"
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
    }
    payload = {
        'grant_type': 'client_credentials',
        'appkey': appkey,
        'secretkey': secretkey
    }

    response = requests.post(url, headers=headers, json=payload)

    print("✅ 응답 코드:", response.status_code)
    try:
        data = response.json()
        print("✅ 응답 내용:", json.dumps(data, indent=4, ensure_ascii=False))
    except:
        print("❌ JSON 파싱 실패. 원본 응답:", response.text)

    return response

# main 실행 구간
if __name__ == '__main__':
    get_token()
