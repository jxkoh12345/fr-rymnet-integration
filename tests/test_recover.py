import os
os.environ.setdefault('HIK_APP_KEY', 'test')
os.environ.setdefault('HIK_APP_SECRET', 'test')
os.environ.setdefault('HIK_BASE_URL', 'http://test')
os.environ.setdefault('HIK_DOOR_EVENTS_PATH', '/test')
os.environ.setdefault('HIK_PERSON_PATH', '/test')
os.environ.setdefault('RYMNET_URL', 'http://test')
os.environ.setdefault('RYMNET_TOKEN', 'test')

import json
import hashlib
from unittest.mock import patch
import pytest

import main
import checkpoint

# Real orphan window file captured from prod: page 2 done, pages [3,4] pending (100 records).
WINDOW_JSON = r'''{"query": {"start": "2026-06-19T08:00:00+08:00", "end": "2026-06-19T08:30:00+08:00", "doors": ["2599", "2604", "2790", "2795", "3245", "4448", "4453", "4458", "4463"], "event_type": 196893}, "page": 2, "pending": {"pages": [3, 4], "records": [{"employee_no": "C0462220", "logtime": "2026-06-19 08:11:35", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "C0462220", "logtime": "2026-06-19 08:11:34", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC14022", "logtime": "2026-06-19 08:11:13", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC14022", "logtime": "2026-06-19 08:11:12", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC9226", "logtime": "2026-06-19 08:11:09", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13055", "logtime": "2026-06-19 08:11:09", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "C0973526", "logtime": "2026-06-19 08:11:08", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC9226", "logtime": "2026-06-19 08:11:08", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13901", "logtime": "2026-06-19 08:10:36", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "C0969026", "logtime": "2026-06-19 08:10:26", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "C0969026", "logtime": "2026-06-19 08:10:24", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC13642", "logtime": "2026-06-19 08:10:17", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "C0978726", "logtime": "2026-06-19 08:10:16", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "RC3650", "logtime": "2026-06-19 08:10:12", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC3650", "logtime": "2026-06-19 08:10:11", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC13379", "logtime": "2026-06-19 08:10:02", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13379", "logtime": "2026-06-19 08:10:01", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC12052", "logtime": "2026-06-19 08:09:52", "indicator": "OUT", "location": "FR (OUT) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC12052", "logtime": "2026-06-19 08:09:50", "indicator": "OUT", "location": "FR (OUT) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC4208", "logtime": "2026-06-19 08:09:47", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC4295", "logtime": "2026-06-19 08:09:22", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC13497", "logtime": "2026-06-19 08:09:06", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC13497", "logtime": "2026-06-19 08:09:05", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC12767", "logtime": "2026-06-19 08:08:49", "indicator": "OUT", "location": "FR MAIN LOBBY SMALL (OUT)_Door_1", "remarks": ""}, {"employee_no": "RC12767", "logtime": "2026-06-19 08:08:33", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC14022", "logtime": "2026-06-19 08:08:09", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC14022", "logtime": "2026-06-19 08:08:08", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "C0860325", "logtime": "2026-06-19 08:08:06", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "FW12304023", "logtime": "2026-06-19 08:08:02", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "FW12304023", "logtime": "2026-06-19 08:08:01", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "C0969026", "logtime": "2026-06-19 08:07:56", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "RC8829", "logtime": "2026-06-19 08:07:38", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC8829", "logtime": "2026-06-19 08:07:36", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "C0254315", "logtime": "2026-06-19 08:07:03", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC3144", "logtime": "2026-06-19 08:06:55", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC14025", "logtime": "2026-06-19 08:06:54", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "RC13631", "logtime": "2026-06-19 08:06:49", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC11739", "logtime": "2026-06-19 08:06:41", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC11739", "logtime": "2026-06-19 08:06:40", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC13137", "logtime": "2026-06-19 08:06:28", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC13740", "logtime": "2026-06-19 08:06:25", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "RC3758", "logtime": "2026-06-19 08:06:13", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD24", "logtime": "2026-06-19 08:06:12", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "GUARD24", "logtime": "2026-06-19 08:06:11", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC8718", "logtime": "2026-06-19 08:06:01", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC8718", "logtime": "2026-06-19 08:06:00", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13137", "logtime": "2026-06-19 08:05:47", "indicator": "OUT", "location": "FR MAIN LOBBY SMALL (OUT)_Door_1", "remarks": ""}, {"employee_no": "RC2627", "logtime": "2026-06-19 08:05:29", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC2627", "logtime": "2026-06-19 08:05:27", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC13745", "logtime": "2026-06-19 08:05:24", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC13745", "logtime": "2026-06-19 08:05:23", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "C0034907", "logtime": "2026-06-19 08:05:20", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13740", "logtime": "2026-06-19 08:05:19", "indicator": "OUT", "location": "TURNSTILE 3", "remarks": ""}, {"employee_no": "C0815924", "logtime": "2026-06-19 08:05:19", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC7221", "logtime": "2026-06-19 08:05:16", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "FW12304023", "logtime": "2026-06-19 08:05:12", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "FW12304023", "logtime": "2026-06-19 08:05:11", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "C0975926", "logtime": "2026-06-19 08:05:05", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "C0975926", "logtime": "2026-06-19 08:05:04", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "GUARD13", "logtime": "2026-06-19 08:04:51", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC2627", "logtime": "2026-06-19 08:04:50", "indicator": "OUT", "location": "FR MAIN LOBBY SMALL (OUT)_Door_1", "remarks": ""}, {"employee_no": "GUARD13", "logtime": "2026-06-19 08:04:50", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "GUARD7", "logtime": "2026-06-19 08:04:40", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC13115", "logtime": "2026-06-19 08:04:31", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC13115", "logtime": "2026-06-19 08:04:31", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "C0647022", "logtime": "2026-06-19 08:04:27", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "C0296216", "logtime": "2026-06-19 08:04:24", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "C0296216", "logtime": "2026-06-19 08:04:23", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:04:22", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:04:20", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:04:20", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "GUARD7", "logtime": "2026-06-19 08:04:18", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:04:17", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:04:16", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD7", "logtime": "2026-06-19 08:04:14", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD13", "logtime": "2026-06-19 08:04:13", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "GUARD7", "logtime": "2026-06-19 08:04:13", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD13", "logtime": "2026-06-19 08:04:12", "indicator": "IN", "location": "TURNSTILE 2", "remarks": ""}, {"employee_no": "RC1638", "logtime": "2026-06-19 08:04:10", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC10050", "logtime": "2026-06-19 08:04:08", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC10170", "logtime": "2026-06-19 08:03:52", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC7221", "logtime": "2026-06-19 08:03:39", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC2648", "logtime": "2026-06-19 08:03:31", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13903", "logtime": "2026-06-19 08:03:24", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "C0562921", "logtime": "2026-06-19 08:03:20", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC3650", "logtime": "2026-06-19 08:03:11", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC8162", "logtime": "2026-06-19 08:03:08", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "RC8162", "logtime": "2026-06-19 08:03:07", "indicator": "IN", "location": "TURNSTILE 4", "remarks": ""}, {"employee_no": "GUARD21", "logtime": "2026-06-19 08:03:07", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "C0819324", "logtime": "2026-06-19 08:03:04", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "C0819324", "logtime": "2026-06-19 08:03:03", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC13903", "logtime": "2026-06-19 08:02:58", "indicator": "OUT", "location": "FR MAIN LOBBY SMALL (OUT)_Door_1", "remarks": ""}, {"employee_no": "RC14748", "logtime": "2026-06-19 08:02:45", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "RC14748", "logtime": "2026-06-19 08:02:44", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}, {"employee_no": "GUARD23", "logtime": "2026-06-19 08:02:44", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "GUARD23", "logtime": "2026-06-19 08:02:42", "indicator": "OUT", "location": "FR - TURNSTILE 1", "remarks": ""}, {"employee_no": "RC9169", "logtime": "2026-06-19 08:02:39", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC13903", "logtime": "2026-06-19 08:02:37", "indicator": "IN", "location": "FR MAIN LOBBY SMALL (IN)_Door_1", "remarks": ""}, {"employee_no": "RC13161", "logtime": "2026-06-19 08:02:36", "indicator": "IN", "location": "FR (IN) LOBBY PARKING STAIRCASE", "remarks": ""}, {"employee_no": "RC2214", "logtime": "2026-06-19 08:02:29", "indicator": "IN", "location": "FR MAIN LOBBY_Door_1", "remarks": ""}]}}'''

DATA = json.loads(WINDOW_JSON)
SIG = DATA['query']


@pytest.fixture
def state(tmp_path, monkeypatch):
    """Point checkpoint at a temp state dir holding the orphan window file.
    Returns the window file Path."""
    monkeypatch.setattr(checkpoint, 'STATE_DIR', str(tmp_path / 'state'))
    monkeypatch.setattr(main, 'DOORS', SIG['doors'])
    monkeypatch.setattr(main, 'EVENT_TYPE', SIG['event_type'])
    windows = tmp_path / 'state' / 'windows'
    windows.mkdir(parents=True)
    h = hashlib.sha1(json.dumps(SIG, sort_keys=True).encode()).hexdigest()[:16]
    wfile = windows / f'{h}.json'
    wfile.write_text(WINDOW_JSON, encoding='utf-8')
    return wfile


def test_filename_matches_signature_hash(state):
    # the captured filename must equal the current code's computed hash
    assert state.name == '0961194335790e04.json'


def test_recover_sends_pending_and_clears(state):
    sent = []
    with patch('main.iter_pages', return_value=iter([])), \
         patch('main.send', side_effect=lambda recs: sent.extend(recs) or {}), \
         patch('main.notify'):
        recovered = main.recover_windows()

    # pending batch resent verbatim (bypasses dedup), in original order
    assert len(sent) == len(DATA['pending']['records'])
    assert sent[0]['employee_no'] == 'C0462220'
    assert sent[-1]['employee_no'] == 'RC2214'
    # no new pages fetched, so reported count is 0
    assert recovered == 0
    # window completed -> file deleted
    assert not state.exists()


def test_recover_skips_windows_still_in_failed_queue(state):
    checkpoint.add_failed(SIG['start'], SIG['end'])  # scheduler still owns it
    sent = []
    with patch('main.iter_pages', return_value=iter([])), \
         patch('main.send', side_effect=lambda recs: sent.extend(recs) or {}), \
         patch('main.notify'):
        recovered = main.recover_windows()

    assert sent == []           # nothing sent
    assert recovered == 0
    assert state.exists()       # orphan left in place for the scheduler


def test_recover_keeps_file_when_send_fails(state):
    def boom(recs):
        raise RuntimeError("rymnet down")

    with patch('main.iter_pages', return_value=iter([])), \
         patch('main.send', side_effect=boom), \
         patch('main.notify'), \
         patch('main.SEND_RETRY_DELAY', 0):
        recovered = main.recover_windows()

    assert recovered == 0
    assert state.exists()       # still failing -> left for next attempt
    # pending preserved for the retry
    assert checkpoint.load_pending(SIG) is not None
