import requests
import json
import math
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
path = os.environ['HIK_DOOR_EVENTS_PATH']
url = os.environ['HIK_BASE_URL'] + path
PAGE_SIZE = 50


def _fetch_page(params: dict, page_no: int) -> dict:
    body = json.dumps({**params, 'pageNo': page_no, 'pageSize': PAGE_SIZE})
    headers = build_headers(app_key, app_secret, path, body)
    print(body)
    res = requests.post(url=url, headers=headers, data=body, verify=False)
    return res.json()


def _build_base_params(
    start_time, end_time, event_type, person_name, person_id, person_code,
    temperature_status, mask_status, sort_field, order_type,
) -> dict:
    raw = {
        'startTime': start_time,
        'endTime': end_time,
        'eventType': event_type,
        'personName': person_name,
        'personId': person_id,
        'personCode': person_code,
        'temperatureStatus': temperature_status,
        'maskStatus': mask_status,
        'sortField': sort_field,
        'orderType': order_type,
    }
    return {k: v for k, v in raw.items() if v != '' and v != []}


def iter_events(
    start_time: str,
    end_time: str,
    event_type: int,
    person_name: str,
    person_id: str,
    person_code: str,
    door_index_codes: list,
    temperature_status: int,
    mask_status: int,
    sort_field: str,
    order_type: int,
):
    """Yields events one by one as pages are fetched from the API."""
    base_params = _build_base_params(
        start_time, end_time, event_type, person_name, person_id, person_code,
        temperature_status, mask_status, sort_field, order_type,
    )
    chunks = [door_index_codes[i:i + 10] for i in range(0, len(door_index_codes), 10)]

    for chunk in chunks:
        params = {**base_params, 'doorIndexCodes': [str(c) for c in chunk]}
        first = _fetch_page(params, 1)
        if str(first.get('code')) != '0':
            raise RuntimeError(f"door_events error: {first.get('msg')}")
        total_pages = math.ceil(first['data']['total'] / PAGE_SIZE)
        yield from first['data']['list']
        for page in range(2, total_pages + 1):
            yield from _fetch_page(params, page)['data']['list']


def iter_pages(
    start_time: str,
    end_time: str,
    event_type: int,
    person_name: str,
    person_id: str,
    person_code: str,
    door_index_codes: list,
    temperature_status: int,
    mask_status: int,
    sort_field: str,
    order_type: int,
    start_page: int = 1,
):
    """Yields (global_page_no, events_list) per page, starting at start_page.

    The API caps doorIndexCodes at 10, so doors are split into chunks of 10.
    Each chunk's pages are laid end-to-end on a single global page counter
    (chunk 0 -> 1..P0, chunk 1 -> P0+1..P0+P1, ...) so page numbers stay
    unambiguous and resumable across chunks. Boundaries are recomputed from
    each chunk's page-1 total, so reruns must see stable window data (they do:
    windows are closed past slots).
    """
    base_params = _build_base_params(
        start_time, end_time, event_type, person_name, person_id, person_code,
        temperature_status, mask_status, sort_field, order_type,
    )
    chunks = [door_index_codes[i:i + 10] for i in range(0, len(door_index_codes), 10)]

    global_page = 0
    for chunk in chunks:
        params = {**base_params, 'doorIndexCodes': [str(c) for c in chunk]}
        first = _fetch_page(params, 1)
        if str(first.get('code')) != '0':
            raise RuntimeError(f"door_events error: {first.get('msg')}")
        total_pages = math.ceil(first['data']['total'] / PAGE_SIZE)
        for local in range(1, total_pages + 1):
            g = global_page + local
            if g < start_page:
                continue
            if local == 1:
                yield g, first['data']['list']
            else:
                res = _fetch_page(params, local)
                if str(res.get('code')) != '0':
                    raise RuntimeError(f"door_events error: {res.get('msg')}")
                yield g, res['data']['list']
        global_page += total_pages


def fetch_all_events(
    start_time: str,
    end_time: str,
    event_type: int,
    person_name: str,
    person_id: str,
    person_code: str,
    door_index_codes: list,
    temperature_status: int,
    mask_status: int,
    sort_field: str,
    order_type: int,
) -> list:
    return list(iter_events(
        start_time, end_time, event_type, person_name, person_id, person_code,
        door_index_codes, temperature_status, mask_status, sort_field, order_type,
    ))


if __name__ == '__main__':
    # --- standalone usage ---
    PARAMS = {
        'startTime': '2026-04-08T08:00:00+08:00',
        'endTime': '2026-04-08T11:00:00+08:00',
        'eventType': 196893,
        'personName': '',
        'personId': '',
        'personCode': '',
        'doorIndexCodes': ['4480'],
        'temperatureStatus': -1,
        'maskStatus': -1,
        'sortField': 'SwipeTime',
        'orderType': 1,
    }

    def build_body(page_no: int) -> str:
        body = {k: v for k, v in PARAMS.items() if v != '' and v != []}
        body['pageNo'] = page_no
        body['pageSize'] = PAGE_SIZE
        return json.dumps(body)

    def fetch_page(page_no: int) -> dict:
        body = build_body(page_no)
        headers = build_headers(app_key, app_secret, path, body)
        res = requests.post(url=url, headers=headers, data=body, verify=False)
        return res.json()

    first = fetch_page(1)
    print(json.dumps(first, indent=2))

    if str(first['code']) == '0':
        total = first['data']['total']
        total_pages = math.ceil(total / PAGE_SIZE)
        print(f"Total records: {total}, pages: {total_pages}")
        all_records = list(first['data']['list'])
        for page in range(2, total_pages + 1):
            data = fetch_page(page)
            all_records.extend(data['data']['list'])
            print(f"Fetched page {page}/{total_pages} ({len(all_records)}/{total})")
        print(f"\nTotal fetched: {len(all_records)}")
        print(json.dumps(all_records, indent=2))
    else:
        print(first['msg'])
