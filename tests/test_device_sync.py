import os
os.environ.setdefault('HIK_APP_KEY', 'test')
os.environ.setdefault('HIK_APP_SECRET', 'test')
os.environ.setdefault('HIK_BASE_URL', 'http://test')
os.environ.setdefault('HIK_DOOR_EVENTS_PATH', '/test')
os.environ.setdefault('HIK_PERSON_PATH', '/test')
os.environ.setdefault('RYMNET_URL', 'http://test')
os.environ.setdefault('RYMNET_TOKEN', 'test')
os.environ.setdefault('ISAPI_USERNAME', 'test')
os.environ.setdefault('ISAPI_PASSWORD', 'test')

from unittest.mock import patch
import pytest

import checkpoint
import device_events
import main

HOST = '10.1.72.119'
INFO = {"doorIndexCode": 4741, "type": "Door", "doorName": "WHCJ OUT - FR", "indicator": "OUT"}


def _auth(serial, time_str, emp):
    return {'major': 5, 'minor': 75, 'serialNo': serial, 'time': time_str,
            'employeeNoString': emp, 'name': 'SOMEONE', 'doorNo': 1, 'cardReaderNo': 1}


def _door(serial, time_str, minor=21):
    return {'major': 5, 'minor': minor, 'serialNo': serial, 'time': time_str, 'doorNo': 1}


def _pages(events, total=None, page=30):
    """Fake _search: serve `events` in pages of `page`, reporting `total`."""
    total = len(events) if total is None else total

    def fake(host, cond, pos=0, limit=page, search_id=None):
        chunk = events[pos:pos + page]
        return {'searchID': search_id, 'totalMatches': total, 'numOfMatches': len(chunk),
                'responseStatusStrg': 'MORE' if pos + len(chunk) < len(events) else 'OK',
                'InfoList': chunk}
    return fake


@pytest.fixture
def state(tmp_path, monkeypatch):
    """Isolate both state dirs and pin DeviceList to a single fake device."""
    monkeypatch.setattr(checkpoint, 'STATE_DIR', str(tmp_path / 'state'))
    monkeypatch.setattr(device_events, 'STATE_DIR', str(tmp_path / 'state' / 'devices'))
    monkeypatch.setattr(device_events, 'PAGE_PACE', 0)
    monkeypatch.setattr(main, 'DeviceList', {HOST: INFO})
    monkeypatch.setattr(main, 'LOG_DIR', str(tmp_path / 'logs'))
    monkeypatch.setattr(main.db, 'insert_records', lambda recs: [None] * len(recs))  # never hit the real DB
    monkeypatch.setattr(main.db, 'set_status', lambda ids, status: None)
    return tmp_path


# --- fetch_range: the two completeness checks -------------------------------

def test_dense_range_returns_all_events():
    events = [_door(100 + i, '2026-08-12T08:00:00+08:00') for i in range(41)]
    with patch.object(device_events, '_search', _pages(events)):
        got = device_events.fetch_range(HOST, 100, 140)
    assert len(got) == 41


def test_gap_in_device_log_raises_and_carries_survivors():
    """totalMatches < width: the device's own log lost events."""
    events = [_door(100 + i, '2026-08-12T08:00:00+08:00') for i in range(39)]
    with patch.object(device_events, '_search', _pages(events)):
        with pytest.raises(device_events.DeviceGapError) as e:
            device_events.fetch_range(HOST, 100, 140)
    assert (e.value.expected, e.value.got) == (41, 39)
    assert len(e.value.events) == 39      # salvageable


def test_short_paging_raises_fetch_error():
    """len(events) < totalMatches: our paging lost rows the device offered.
    This is the searchID-reuse failure — device claims 41, hands over 14."""
    events = [_door(100 + i, '2026-08-12T08:00:00+08:00') for i in range(14)]
    with patch.object(device_events, '_search', _pages(events, total=41)):
        with pytest.raises(device_events.DeviceFetchError):
            device_events.fetch_range(HOST, 100, 140)


def test_paging_walks_every_page():
    events = [_door(1000 + i, '2026-08-12T08:00:00+08:00') for i in range(95)]
    with patch.object(device_events, '_search', _pages(events)):
        got = device_events.fetch_range(HOST, 1000, 1094)
    assert [e['serialNo'] for e in got] == [1000 + i for i in range(95)]


def test_auth_events_filters_and_sorts():
    raw = [_door(3, '2026-08-12T08:00:05+08:00', minor=22),
           _auth(2, '2026-08-12T08:00:09+08:00', 'RC2'),
           _door(4, '2026-08-12T08:00:01+08:00', minor=21),
           _auth(1, '2026-08-12T08:00:00+08:00', 'RC1'),
           {'major': 3, 'minor': 112, 'serialNo': 5, 'time': '2026-08-12T08:00:02+08:00'}]
    assert [e['employeeNoString'] for e in device_events.auth_events(raw)] == ['RC1', 'RC2']


# --- resolve parity ---------------------------------------------------------

def test_device_record_matches_artemis_record():
    """Same physical event, both resolvers — identical Rymnet body. Values are a
    real captured pair (device employeeNoString == Artemis personCode)."""
    device_event = _auth(134135, '2026-08-12T07:46:16+08:00', 'RC13641')
    artemis_event = {'personId': '906', 'eventTime': '2026-08-12T07:46:16+08:00',
                     'doorIndexCode': '4741'}
    with patch.dict(main.DoorList, {4741: INFO}, clear=False), \
         patch('main.fetch_person_info', return_value={'personCode': 'RC13641'}):
        assert main._resolve_device_record(device_event, INFO) == \
               main._resolve_record(artemis_event, {})


# --- cursor lifecycle ------------------------------------------------------

def test_first_run_bootstraps_to_newest_without_sending(state):
    sent = []
    with patch.object(device_events, 'newest_serial', return_value=(500, '2026-08-12T08:00:00+08:00')), \
         patch('main.send', side_effect=lambda r: sent.extend(r) or {}), \
         patch('main.notify'):
        ok, n = main.run_device_cycle(HOST)
    assert (ok, n, sent) == (True, 0, [])
    assert device_events.load_state(HOST)['cursor'] == 500


def test_cycle_sends_auth_events_and_advances_cursor(state):
    device_events.save_state(HOST, 100)
    events = [_auth(101, '2026-08-12T08:00:00+08:00', 'RC1'),
              _door(102, '2026-08-12T08:00:01+08:00'),
              _auth(103, '2026-08-12T08:10:00+08:00', 'FWA12345')]
    sent = []
    with patch.object(device_events, 'newest_serial', return_value=(103, '2026-08-12T08:10:00+08:00')), \
         patch.object(device_events, '_search', _pages(events)), \
         patch('main.send', side_effect=lambda r: sent.extend(r) or {}), \
         patch('main.notify'):
        ok, n = main.run_device_cycle(HOST)
    assert (ok, n) == (True, 2)
    assert [r['employee_no'] for r in sent] == ['RC1', 'FW-A12345']   # FW prefix normalized
    assert sent[0]['location'] == 'WHCJ OUT - FR' and sent[0]['indicator'] == 'OUT'
    assert device_events.load_state(HOST)['cursor'] == 103


def test_send_failure_leaves_cursor_put_and_saves_pending(state):
    device_events.save_state(HOST, 100)
    events = [_auth(101, '2026-08-12T08:00:00+08:00', 'RC1')]
    with patch.object(device_events, 'newest_serial', return_value=(101, '2026-08-12T08:00:00+08:00')), \
         patch.object(device_events, '_search', _pages(events)), \
         patch('main.send', side_effect=RuntimeError('rymnet down')), \
         patch('main.SEND_RETRY_DELAY', 0), \
         patch('main.notify'):
        ok, n = main.run_device_cycle(HOST)
    assert (ok, n) == (False, 0)
    assert device_events.load_state(HOST)['cursor'] == 100      # span refetched next cycle
    assert checkpoint.load_pending(main._device_signature(HOST))['records'][0]['employee_no'] == 'RC1'


def test_pending_batch_retried_before_new_events(state):
    device_events.save_state(HOST, 100)
    sig = main._device_signature(HOST)
    checkpoint.save_pending(sig, [1], [{'employee_no': 'RC9', 'logtime': '2026-08-12 07:00:00',
                                        'indicator': 'OUT', 'location': 'WHCJ OUT - FR', 'remarks': ''}], [None])
    events = [_auth(101, '2026-08-12T08:00:00+08:00', 'RC1')]
    sent = []
    with patch.object(device_events, 'newest_serial', return_value=(101, '2026-08-12T08:00:00+08:00')), \
         patch.object(device_events, '_search', _pages(events)), \
         patch('main.send', side_effect=lambda r: sent.extend(r) or {}), \
         patch('main.notify'):
        ok, n = main.run_device_cycle(HOST)
    assert ok
    assert [r['employee_no'] for r in sent] == ['RC9', 'RC1']   # pending first, no double-send
    assert checkpoint.load_pending(sig) is None


def test_device_log_reset_does_not_reconsume(state):
    """Counter below the cursor = wiped log. Must alert and leave state alone."""
    device_events.save_state(HOST, 134000)
    sent = []
    with patch.object(device_events, 'newest_serial', return_value=(29, '2026-01-01T13:37:28+08:00')), \
         patch('main.send', side_effect=lambda r: sent.extend(r) or {}), \
         patch('main.notify') as notified:
        ok, n = main.run_device_cycle(HOST)
    assert (ok, n, sent) == (False, 0, [])
    assert device_events.load_state(HOST)['cursor'] == 134000
    assert 'RESET' in notified.call_args[0][0]


def test_gap_is_logged_then_survivors_sent(state, monkeypatch):
    monkeypatch.chdir(state)          # errors/ is written relative to cwd
    device_events.save_state(HOST, 100)
    events = [_auth(101, '2026-08-12T08:00:00+08:00', 'RC1')]      # 105 requested, 1 present
    sent = []
    with patch.object(device_events, 'newest_serial', return_value=(105, '2026-08-12T08:00:00+08:00')), \
         patch.object(device_events, '_search', _pages(events, total=1)), \
         patch('main.send', side_effect=lambda r: sent.extend(r) or {}), \
         patch('main.notify'):
        ok, n = main.run_device_cycle(HOST)
    assert (ok, n) == (True, 1)
    assert [r['employee_no'] for r in sent] == ['RC1']
    assert device_events.load_state(HOST)['cursor'] == 105        # gap recorded, not retried forever
    assert list((state / 'errors').glob('device_gap_*.json'))


def test_nothing_new_is_a_noop(state):
    device_events.save_state(HOST, 500)
    with patch.object(device_events, 'newest_serial', return_value=(500, '2026-08-12T08:00:00+08:00')), \
         patch('main.send', side_effect=AssertionError('must not send')):
        assert main.run_device_cycle(HOST) == (True, 0)


def test_dedup_memory_survives_across_cycles(state):
    """Two auths seconds apart either side of a cycle boundary: the second must
    still be suppressed, so last_sent has to persist in the cursor file."""
    device_events.save_state(HOST, 100)
    first = [_auth(101, '2026-08-12T08:00:00+08:00', 'RC1')]
    second = [_auth(102, '2026-08-12T08:00:30+08:00', 'RC1')]
    sent = []
    with patch('main.send', side_effect=lambda r: sent.extend(r) or {}), patch('main.notify'):
        with patch.object(device_events, 'newest_serial', return_value=(101, '2026-08-12T08:00:00+08:00')), \
             patch.object(device_events, '_search', _pages(first)):
            main.run_device_cycle(HOST)
        assert device_events.load_state(HOST)['last_sent'] == {'RC1|WHCJ - FR': '2026-08-12 08:00:00'}
        with patch.object(device_events, 'newest_serial', return_value=(102, '2026-08-12T08:00:30+08:00')), \
             patch.object(device_events, '_search', _pages(second)):
            ok, n = main.run_device_cycle(HOST)
    assert (ok, n) == (True, 0)                 # suppressed: < MIN_GAP_MINUTES since 08:00:00
    assert len(sent) == 1


def test_recover_windows_ignores_device_state(state):
    """A device's pending state lives in state/windows/ too, but it is not a time
    window — recover_windows must not try to re-run it through the Artemis path."""
    sig = main._device_signature(HOST)
    checkpoint.save_pending(sig, [1], [{'employee_no': 'RC9'}], [None])
    with patch('main.run_window', side_effect=AssertionError('device state is not a window')):
        assert main.recover_windows() == 0
