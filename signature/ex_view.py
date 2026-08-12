"""Read-only viewer for the FR devices' own event log (WHCJ IN / OUT).

Nothing is written to the devices. Experiment script — not part of the pipeline.

    uv run ex_view.py tail                          newest 20 auth events, both devices
    uv run ex_view.py tail 50 --host 10.1.72.119    newest 50, one device
    uv run ex_view.py day 2026-08-12                every auth event that day
    uv run ex_view.py serial 134300 134400          raw serial range + contiguity check
    uv run ex_view.py cursor                        newest serial per device (cursor probe)

    --all   include non-auth events (door open/close, tamper, ...) instead of only
            successful authentications
"""
import os
import sys
import json
import time
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

# device IP -> Artemis doorIndexCode / DoorList name, confirmed by event parity
DEVICES = {
    '10.1.72.122': (4729, 'WHCJ IN  - FR'),
    '10.1.72.119': (4741, 'WHCJ OUT - FR'),
}

AUTH_MAJOR, AUTH_MINOR = 5, 75   # successful authentication (carries employeeNoString)
PAGE = 30                        # device caps maxResults at 30
PACE = 0.4                       # seconds between pages; devices 401 under rapid fire

# observed pairing around an auth event: 75 then 21 (unlock) then 22 (lock)
MINOR_LABEL = {75: 'auth ok', 21: 'door open', 22: 'door close'}

_sessions = {}
_seq = [0]


def _session(host):
    """One Session per device: keeps the digest nonce alive across pages. A fresh
    HTTPDigestAuth per request makes the device start answering 401."""
    if host not in _sessions:
        s = requests.Session()
        s.auth = requests.auth.HTTPDigestAuth(
            os.environ['ISAPI_USERNAME'], os.environ['ISAPI_PASSWORD'])
        _sessions[host] = s
    return _sessions[host]


def search(host, cond, pos=0, limit=PAGE, search_id=None):
    """POST one AcsEvent search page. Retries the two failure modes the devices
    actually show: empty HTTP 200 bodies, and 401 once the nonce goes stale.

    Pass a stable search_id for every page of one result set — a fresh searchID
    per page makes the device answer from the wrong cached search (observed: a
    2-day query truncating at 41 of 1148 events)."""
    if search_id is None:
        _seq[0] += 1
        search_id = f'view{_seq[0]}'
    body = {'AcsEventCond': {
        'searchID': search_id,
        'searchResultPosition': pos,
        'maxResults': limit,
        **cond,
    }}
    for attempt in range(5):
        r = _session(host).post(
            f'http://{host}/ISAPI/AccessControl/AcsEvent?format=json',
            json=body, timeout=30)
        if r.status_code == 200 and r.text.strip():
            return json.loads(r.text)['AcsEvent']
        if r.status_code == 401:
            _sessions.pop(host, None)
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f'{host} pos={pos}: failed 5 attempts, last {r.status_code}')


def fetch_all(host, cond):
    """Page through a search until the device stops saying MORE."""
    _seq[0] += 1
    sid = f'page{_seq[0]}'
    out, pos = [], 0
    while True:
        d = search(host, cond, pos=pos, search_id=sid)
        out += d.get('InfoList', [])
        pos += d.get('numOfMatches', 0)
        if d.get('responseStatusStrg') != 'MORE' or not d.get('numOfMatches'):
            return out, d.get('totalMatches', 0)
        time.sleep(PACE)


def _filter(auth_only):
    return {'major': AUTH_MAJOR, 'minor': AUTH_MINOR} if auth_only else {'major': 0, 'minor': 0}


def show(host, events):
    door, name = DEVICES.get(host, ('?', host))
    for e in sorted(events, key=lambda v: (v['time'], v['serialNo'])):
        label = MINOR_LABEL.get(e['minor'], f"minor {e['minor']}")
        emp = e.get('employeeNoString') or '-'
        who = (e.get('name') or '')[:24]
        print(f"{e['serialNo']:>8}  {e['time'][:19]}  {name}  door={door}  "
              f"{label:<10}  {emp:<14} {who}")


def cmd_cursor(hosts):
    """Newest serial per device — this is what a cursor-based poller would store."""
    for host in hosts:
        d = search(host, {**_filter(False), 'timeReverseOrder': True}, limit=1)
        e = d['InfoList'][0]
        door, name = DEVICES[host]
        print(f"{host}  {name}  door={door}  newest serial={e['serialNo']}  at {e['time'][:19]}")
        time.sleep(PACE)


def cmd_tail(hosts, count, auth_only):
    for host in hosts:
        d = search(host, {**_filter(auth_only), 'timeReverseOrder': True},
                   limit=min(count, PAGE))
        show(host, d.get('InfoList', []))
        print()
        time.sleep(PACE)


def cmd_day(hosts, day, auth_only):
    for host in hosts:
        cond = {**_filter(auth_only),
                'startTime': f'{day}T00:00:00+08:00',
                'endTime': f'{day}T23:59:59+08:00'}
        events, total = fetch_all(host, cond)
        door, name = DEVICES[host]
        print(f"--- {host} {name} door={door} {day}: {len(events)} events (device total {total})")
        show(host, events)
        print()


def cmd_serial(hosts, begin, end, auth_only):
    """Both bounds are mandatory and inclusive — beginSerialNo alone is a 400.
    Unfiltered, the serial space is dense, so total vs width reveals lost events."""
    width = end - begin + 1
    for host in hosts:
        cond = {**_filter(auth_only), 'beginSerialNo': begin, 'endSerialNo': end}
        events, total = fetch_all(host, cond)
        door, name = DEVICES[host]
        print(f"--- {host} {name} door={door} serial {begin}-{end}: "
              f"{len(events)} fetched, device total {total}")
        if not auth_only:
            gap = width - total
            print(f"    contiguity: width {width} vs total {total} -> "
                  + ('dense, no holes' if gap == 0 else f'{gap} MISSING'))
        show(host, events)
        print()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('cmd', choices=('tail', 'day', 'serial', 'cursor'))
    p.add_argument('args', nargs='*')
    p.add_argument('--host', action='append', help='limit to one device IP (repeatable)')
    p.add_argument('--all', action='store_true', help='include non-auth events')
    a = p.parse_args()

    hosts = a.host or list(DEVICES)
    auth_only = not a.all

    if a.cmd == 'cursor':
        cmd_cursor(hosts)
    elif a.cmd == 'tail':
        cmd_tail(hosts, int(a.args[0]) if a.args else 20, auth_only)
    elif a.cmd == 'day':
        cmd_day(hosts, a.args[0], auth_only)
    elif a.cmd == 'serial':
        cmd_serial(hosts, int(a.args[0]), int(a.args[1]), auth_only)
