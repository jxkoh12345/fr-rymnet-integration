import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
URL = os.environ['RYMNET_URL']
BEARER_TOKEN = os.environ['RYMNET_TOKEN']


def build_body(employee_no: str, logtime: str, location: str, indicator: str = '', remarks: str = '') -> dict:
    return {
        'employee_no': employee_no,
        'logtime': logtime,
        'indicator': indicator,
        'location': location,
        'remarks': remarks,
    }


def send(records: list) -> dict:
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {BEARER_TOKEN}',
    }
    res = requests.post(URL, headers=headers, data=json.dumps(records))
    res.raise_for_status()
    return res.json()
