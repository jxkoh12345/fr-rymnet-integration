"""Scratch: probe the FR device directly (read-only) to see if live/device data can
reproduce the same events the Artemis-server pipeline gets."""
import os
import json
import sys
import time
import requests
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv('DEVICE_HOST', 'http://10.1.250.62:80')
AUTH = requests.auth.HTTPDigestAuth(os.getenv('ISAPI_USERNAME'), os.getenv('ISAPI_PASSWORD'))


_sessions = {}


def _session():
    """One Session per host: keeps the digest nonce alive with a proper nc
    counter — a fresh challenge per request makes the device answer 401."""
    if HOST not in _sessions:
        s = requests.Session()
        s.auth = requests.auth.HTTPDigestAuth(
            os.getenv('ISAPI_USERNAME'), os.getenv('ISAPI_PASSWORD'))
        _sessions[HOST] = s
    return _sessions[HOST]


def get(path):
    r = _session().get(HOST + path, timeout=15)
    return r.status_code, r.text[:2000]


def post(path, body):
    r = _session().post(HOST + path, json=body, timeout=30)
    return r.status_code, r.text


_seq = [0]


def acs_event(start, end, pos=0, max_results=30, major=5, minor=75):
    """Device intermittently answers HTTP 200 with an empty body — retry."""
    _seq[0] += 1
    body = {"AcsEventCond": {
        "searchID": f"ex{_seq[0]}",
        "searchResultPosition": pos,
        "maxResults": max_results,
        "major": major, "minor": minor,
        "startTime": start, "endTime": end,
    }}
    for attempt in range(5):
        c, t = post('/ISAPI/AccessControl/AcsEvent?format=json', body)
        if c == 200 and t.strip():
            return c, t
        if c == 401:
            _sessions.pop(HOST, None)   # nonce dead — new session, new challenge
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f'pos={pos} failed after 5 attempts, last status {c}')


def users(pos=0, max_results=5):
    body = {"UserInfoSearchCond": {
        "searchID": "ex2", "searchResultPosition": pos, "maxResults": max_results,
    }}
    return post('/ISAPI/AccessControl/UserInfo/Search?format=json', body)


def device_auth_events(start, end):
    """All minor-75 (face auth passed) events in a window, paged."""
    out, pos = [], 0
    while True:
        c, t = acs_event(start, end, pos=pos, max_results=30)
        try:
            d = json.loads(t)['AcsEvent']
        except json.JSONDecodeError:
            print(f'BAD BODY status={c} pos={pos} len={len(t)}\n{t[:300]!r}\n...{t[-200:]!r}')
            raise
        for e in d.get('InfoList', []):
            if e.get('employeeNoString'):
                out.append(e)
        pos += d.get('numOfMatches', 0)
        if d.get('responseStatusStrg') != 'MORE' or d.get('numOfMatches', 0) == 0:
            return out


def artemis_events(start, end):
    from signature.door_events import iter_events
    from DoorList import DoorList
    doors = [str(k) for k, v in DoorList.items() if v['type'] == 'Door']
    return list(iter_events(start, end, 196893, '', '', '', doors, -1, -1, 'SwipeTime', 0))


if __name__ == '__main__':
    what = sys.argv[1] if len(sys.argv) > 1 else 'info'
    if what == 'info':
        for p in ('/ISAPI/System/deviceInfo?format=json',
                  '/ISAPI/System/time?format=json',
                  '/ISAPI/AccessControl/AcsEvent/capabilities?format=json'):
            c, t = get(p)
            print(f'--- {p} -> {c}\n{t}\n')
    elif what == 'events':
        start = sys.argv[2] if len(sys.argv) > 2 else '2026-08-12T00:00:00+08:00'
        end = sys.argv[3] if len(sys.argv) > 3 else '2026-08-12T23:59:59+08:00'
        c, t = acs_event(start, end, max_results=int(os.getenv('MAXR', '5')))
        print(c)
        try:
            print(json.dumps(json.loads(t), indent=2, ensure_ascii=False))
        except Exception:
            print(t)
    elif what == 'compare':
        start, end = sys.argv[2], sys.argv[3]
        dev = device_auth_events(start, end)
        print(f'device auth events: {len(dev)}')
        for e in dev:
            print(f"  DEV {e['time']} {e['employeeNoString']:14} {e.get('name','')[:22]:22} "
                  f"door={e.get('doorNo')} reader={e.get('cardReaderNo')} minor={e['minor']}")
        art = artemis_events(start, end)
        print(f'artemis events: {len(art)}')
        for e in art:
            print(f"  ART {e.get('eventTime')} pid={e.get('personId')} door={e.get('doorIndexCode')} "
                  f"name={e.get('personName','')} type={e.get('eventType')}")
    elif what == 'users':
        c, t = users()
        print(c, t)

# parity: device vs artemis for one door
def parity(start, end, door):
    dev = device_auth_events(start, end)
    from signature.door_events import iter_events
    from signature.personId import fetch_person_info
    art = list(iter_events(start, end, 196893, '', '', '', [str(door)], -1, -1, 'SwipeTime', 0))
    cache = {}
    def code(pid):
        if pid not in cache:
            cache[pid] = fetch_person_info(pid).get('personCode', '')
        return cache[pid]
    D = {(e['employeeNoString'], e['time'][:19]) for e in dev}
    A = {(code(e['personId']), e['eventTime'][:19]) for e in art}
    print(f'device={len(D)} artemis={len(A)} both={len(D&A)}')
    print('device only:', sorted(D - A)[:20])
    print('artemis only:', sorted(A - D)[:20])
