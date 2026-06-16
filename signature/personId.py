import requests
import json
import os
from dotenv import load_dotenv
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from signature.auth import build_headers
except ImportError:
    from auth import build_headers

load_dotenv()
app_key = os.environ['HIK_APP_KEY']
app_secret = os.environ['HIK_APP_SECRET']
path = os.environ['HIK_PERSON_PATH']
url = os.environ['HIK_BASE_URL'] + path


def fetch_person_info(person_id: str) -> dict:
    body = json.dumps({'personId': person_id})
    headers = build_headers(app_key, app_secret, path, body)
    res = requests.post(url=url, headers=headers, data=body, verify=False)
    data = res.json()
    if str(data.get('code')) != '0':
        raise RuntimeError(f"personId API error for {person_id}: {data.get('msg')}")
    return data['data']


if __name__ == '__main__':
    import sys
    pid = sys.argv[1] if len(sys.argv) > 1 else '1203'
    try:
        info = fetch_person_info(pid)
        print(json.dumps(info, indent=2))
    except RuntimeError as e:
        print(e)
