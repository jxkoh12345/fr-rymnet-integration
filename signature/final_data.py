import requests
import json
import os
import time
from collections import deque
from dotenv import load_dotenv

load_dotenv()
URL = os.environ['RYMNET_URL']
BEARER_TOKEN = os.environ['RYMNET_TOKEN']

# Rymnet allows 10 calls per 60s; throttle to stay under it (sliding window).
RATE_LIMIT = 10
RATE_WINDOW = 60.0
_call_times: deque = deque()


def _throttle():
    """Block until sending now keeps us within RATE_LIMIT calls per RATE_WINDOW."""
    now = time.monotonic()
    while _call_times and now - _call_times[0] >= RATE_WINDOW:
        _call_times.popleft()
    if len(_call_times) >= RATE_LIMIT:
        wait = RATE_WINDOW - (now - _call_times[0])
        if wait > 0:
            time.sleep(wait)
        _call_times.popleft()
    _call_times.append(time.monotonic())


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
    _throttle()
    res = requests.post(URL, headers=headers, data=json.dumps(records))
    try:
        res.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(f"{e} — response body: {res.text[:500]}", response=res) from None
    return res.json()
