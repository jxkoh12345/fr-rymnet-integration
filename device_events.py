"""Read events straight from the FR terminals over ISAPI, indexed by the device's
own event counter (`serialNo`) instead of a time window.

Why serials, not time: the server-side pipeline asks "what happened between
08:00 and 08:30?". If an event should have been there but wasn't (device→server
upload lag), the answer is indistinguishable from "nothing happened" — missed
data is invisible and unrecoverable. Every device event instead carries a
monotonic, gap-free counter shared by all event types, so a fetch asks for a
span of numbered slots and can *prove* nothing is missing:

  width = end - begin + 1        slots asked for
  totalMatches == width          the device's log has no holes
  len(events)  == totalMatches   paging retrieved everything offered

Both checks are enforced by fetch_range(). Verified dense on both terminals:
1000-wide spans returned exactly 1000.

State is one file per device holding the cursor (last serial fully processed),
so a poller that was down simply asks for a wider span next time.
"""
import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

STATE_DIR = os.path.join('state', 'devices')

AUTH_MAJOR, AUTH_MINOR = 5, 75   # successful authentication: the only event carrying employeeNoString
PAGE_SIZE = 30                   # device caps maxResults at 30
PAGE_PACE = 0.4                  # seconds between pages; devices answer 401 under rapid fire
ATTEMPTS  = 5


class DeviceFetchError(RuntimeError):
    """Device unreachable, or paging returned less than the device offered."""


class DeviceGapError(RuntimeError):
    """The device's own log is missing serials in the requested span (events
    lost or aged out). Carries what arrived so the caller can salvage it."""

    def __init__(self, message, events, expected, got):
        super().__init__(message)
        self.events = events
        self.expected = expected
        self.got = got


class DeviceResetError(RuntimeError):
    """Newest serial is below our cursor — the device's event log was wiped and
    its counter restarted. Never re-consume from 1 silently."""


_sessions = {}
_seq = [0]


def _session(host: str) -> requests.Session:
    """One Session per device: keeps the digest nonce alive across pages. A fresh
    HTTPDigestAuth per request makes the device start answering 401."""
    if host not in _sessions:
        s = requests.Session()
        s.auth = requests.auth.HTTPDigestAuth(
            os.environ['ISAPI_USERNAME'], os.environ['ISAPI_PASSWORD'])
        _sessions[host] = s
    return _sessions[host]


def _search(host: str, cond: dict, pos: int = 0, limit: int = PAGE_SIZE, search_id: str = None) -> dict:
    """POST one AcsEvent search page.

    search_id must stay stable across the pages of one result set — a fresh
    searchID per page makes the device answer from a different cached search
    (observed: a 1148-event query silently truncating to 41, self-consistent
    totalMatches included).

    Retries the two failure modes the devices actually show: empty HTTP 200
    bodies, and 401 once the digest nonce goes stale.
    """
    if search_id is None:
        _seq[0] += 1
        search_id = f'sync{_seq[0]}'
    body = {'AcsEventCond': {
        'searchID': search_id,
        'searchResultPosition': pos,
        'maxResults': limit,
        **cond,
    }}
    status = None
    for attempt in range(ATTEMPTS):
        try:
            r = _session(host).post(
                f'http://{host}/ISAPI/AccessControl/AcsEvent?format=json',
                json=body, timeout=30)
        except requests.RequestException as e:
            status = str(e)
        else:
            status = r.status_code
            if r.status_code == 200 and r.text.strip():
                return json.loads(r.text)['AcsEvent']
            if r.status_code == 401:
                _sessions.pop(host, None)   # stale nonce — new session, new challenge
        time.sleep(1.5 * (attempt + 1))
    raise DeviceFetchError(f"{host} pos={pos}: {ATTEMPTS} attempts failed, last status {status}")


def newest_serial(host: str) -> tuple[int, str]:
    """(serialNo, time) of the device's most recent event — the fetch upper bound."""
    d = _search(host, {'major': 0, 'minor': 0, 'timeReverseOrder': True}, limit=1)
    info = d.get('InfoList') or []
    if not info:
        raise DeviceFetchError(f"{host}: device log is empty")
    return info[0]['serialNo'], info[0]['time']


def fetch_range(host: str, begin: int, end: int) -> list:
    """Every event with begin <= serialNo <= end, unfiltered.

    Unfiltered on purpose: the completeness check needs the dense counter. Asking
    the device for only minor 75 would return 14 of a 41-wide span every time,
    making the comparison meaningless — so filter with auth_events() afterwards.

    Both serial bounds are mandatory and inclusive; beginSerialNo alone is a 400.
    Raises DeviceGapError if the device's log has holes in the span.
    """
    cond = {'major': 0, 'minor': 0, 'beginSerialNo': begin, 'endSerialNo': end}
    _seq[0] += 1
    sid = f'range{_seq[0]}'
    events, pos, total = [], 0, 0
    while True:
        d = _search(host, cond, pos=pos, search_id=sid)
        events += d.get('InfoList', [])
        total = d.get('totalMatches', 0)
        got = d.get('numOfMatches', 0)
        pos += got
        if d.get('responseStatusStrg') != 'MORE' or not got:
            break
        time.sleep(PAGE_PACE)

    if len(events) != total:            # our paging lost rows the device offered
        raise DeviceFetchError(
            f"{host} serial {begin}-{end}: fetched {len(events)} but device reported {total}")

    width = end - begin + 1
    if total != width:                  # the device's own log is short
        raise DeviceGapError(
            f"{host} serial {begin}-{end}: expected {width} events, device has {total} "
            f"({width - total} missing from the device log)",
            events, width, total)
    return events


def auth_events(events: list) -> list:
    """Successful authentications only, oldest first. Response order is not serial
    order (an auth and its door-open event share a second and can arrive either
    way round), so sort explicitly."""
    auth = [e for e in events
            if e.get('major') == AUTH_MAJOR and e.get('minor') == AUTH_MINOR
            and e.get('employeeNoString')]
    return sorted(auth, key=lambda e: (e['time'], e['serialNo']))


# --- cursor state -----------------------------------------------------------

def _state_path(host: str) -> str:
    return os.path.join(STATE_DIR, f"{host.replace(':', '_')}.json")


def load_state(host: str) -> dict:
    """{'cursor': int|None, 'last_sent': {'<employee_no>|<device>': '<iso>'}}

    cursor None = never polled (caller bootstraps). last_sent carries the dedup
    memory across cycles: it lives in-process for a window run, but device polls
    are short and frequent, so without persisting it two events a few seconds
    apart either side of a cycle boundary would both pass the MIN_GAP check.
    """
    try:
        with open(_state_path(host), encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {'cursor': None, 'last_sent': {}}
    return {'cursor': data.get('cursor'), 'last_sent': data.get('last_sent', {})}


def save_state(host: str, cursor: int, last_sent: dict = None):
    os.makedirs(STATE_DIR, exist_ok=True)
    payload = {
        'device': host,
        'cursor': cursor,
        'updated': time.strftime('%Y-%m-%d %H:%M:%S'),
        'last_sent': last_sent or {},
    }
    tmp = _state_path(host) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, _state_path(host))
