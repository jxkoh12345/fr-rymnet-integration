import os
os.environ.setdefault('HIK_APP_KEY', 'test')
os.environ.setdefault('HIK_APP_SECRET', 'test')
os.environ.setdefault('HIK_BASE_URL', 'http://test')
os.environ.setdefault('HIK_DOOR_EVENTS_PATH', '/test')
os.environ.setdefault('HIK_PERSON_PATH', '/test')
os.environ.setdefault('RYMNET_URL', 'http://test')
os.environ.setdefault('RYMNET_TOKEN', 'test')

from unittest.mock import patch
import main


def _run(events, foreign_worker: bool):
    sent = []
    with patch('main.iter_pages', return_value=iter([(1, events)])), \
         patch('main._resolve_record', side_effect=lambda item, cache: item), \
         patch('main.send', side_effect=lambda records: sent.extend(records) or {}), \
         patch('main.checkpoint.query_signature', return_value={}), \
         patch('main.checkpoint.load_pending', return_value=None), \
         patch('main.checkpoint.load_checkpoint', return_value=0), \
         patch('main.checkpoint.save_page'), \
         patch('main.checkpoint.clear_window'), \
         patch('main.notify'), \
         patch.object(main, 'FOREIGN_WORKER', foreign_worker):
        main.run_window('2026-01-01T08:00:00+08:00', '2026-01-01T08:30:00+08:00')
    return sent


def r(emp, logtime='2026-01-01 08:00:00'):
    return {'employee_no': emp, 'logtime': logtime, 'location': '', 'indicator': '', 'remarks': ''}


# --- switch OFF (default) ---

def test_off_sends_all():
    sent = _run([r('RC3645'), r('FWA00304480'), r('E001')], foreign_worker=False)
    assert len(sent) == 3


def test_off_sends_non_fw():
    sent = _run([r('RC3645')], foreign_worker=False)
    assert len(sent) == 1


# --- switch ON ---

def test_on_fw_prefix_sent():
    sent = _run([r('FWA00304480')], foreign_worker=True)
    assert len(sent) == 1


def test_on_non_fw_filtered():
    sent = _run([r('RC3645')], foreign_worker=True)
    assert len(sent) == 0


def test_on_mixed_only_fw_sent():
    sent = _run([r('RC3645'), r('FWA00304480'), r('FWB00100001')], foreign_worker=True)
    assert len(sent) == 2
    assert all(s['employee_no'].startswith('FW-') for s in sent)


# --- dash insertion ---

def test_on_dash_inserted():
    sent = _run([r('FWBT0490848')], foreign_worker=True)
    assert sent[0]['employee_no'] == 'FW-BT0490848'


def test_on_dash_not_inserted_when_off():
    sent = _run([r('FWBT0490848')], foreign_worker=False)
    assert sent[0]['employee_no'] == 'FWBT0490848'


def test_on_dash_inserted_multiple():
    sent = _run([r('FWA00304480'), r('FWBT0490848')], foreign_worker=True)
    assert sent[0]['employee_no'] == 'FW-A00304480'
    assert sent[1]['employee_no'] == 'FW-BT0490848'


def test_on_fw_only_becomes_fw_dash():
    # "FW" with nothing after → "FW-"
    sent = _run([r('FW')], foreign_worker=True)
    assert sent[0]['employee_no'] == 'FW-'


def test_on_empty_employee_no_filtered():
    # blank employee_no does not start with FW → filtered
    sent = _run([r('')], foreign_worker=True)
    assert len(sent) == 0


def test_on_fw_lowercase_filtered():
    # prefix check is case-sensitive; "fw..." must not pass
    sent = _run([r('fwA00304480')], foreign_worker=True)
    assert len(sent) == 0


def test_on_all_non_fw_zero_sent():
    sent = _run([r('RC3645'), r('E001'), r('A999')], foreign_worker=True)
    assert len(sent) == 0


def test_on_fw_prefix_only_boundary():
    # exactly "FW" with nothing after it is still a valid prefix
    sent = _run([r('FW')], foreign_worker=True)
    assert len(sent) == 1
