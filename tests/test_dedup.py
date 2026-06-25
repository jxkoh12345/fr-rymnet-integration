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


def _run(events):
    """Run run_window with a single page of pre-resolved records. Returns sent records."""
    sent = []

    with patch('main.iter_pages', return_value=iter([(1, events)])), \
         patch('main._resolve_record', side_effect=lambda item, cache: item), \
         patch('main.send', side_effect=lambda records: sent.extend(records) or {}), \
         patch('main.checkpoint.query_signature', return_value={}), \
         patch('main.checkpoint.load_pending', return_value=None), \
         patch('main.checkpoint.load_checkpoint', return_value=0), \
         patch('main.checkpoint.save_page'), \
         patch('main.checkpoint.clear_window'), \
         patch('main.notify'):
        main.run_window('2026-01-01T08:00:00+08:00', '2026-01-01T08:30:00+08:00')

    return sent


def r(emp, logtime):
    return {'employee_no': emp, 'logtime': logtime, 'location': '', 'indicator': '', 'remarks': ''}


# --- same employee ---

def test_exact_duplicate_skipped():
    e = r('E001', '2026-01-01 08:00:00')
    assert len(_run([e, e])) == 1


def test_within_5min_skipped():
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E001', '2026-01-01 08:04:59')])
    assert len(sent) == 1


def test_exactly_5min_sent():
    # boundary: 300s is NOT < 300, so both are sent
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E001', '2026-01-01 08:05:00')])
    assert len(sent) == 2


def test_over_5min_sent():
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E001', '2026-01-01 08:06:00')])
    assert len(sent) == 2


def test_third_event_gap_resets_per_send():
    # 08:00 sent, 08:03 skipped, 08:07 compared against 08:00 → 7min gap → sent
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E001', '2026-01-01 08:03:00'),
                 r('E001', '2026-01-01 08:07:00')])
    assert len(sent) == 2
    assert sent[1]['logtime'] == '2026-01-01 08:07:00'


# --- different employees ---

def test_different_employees_same_logtime_both_sent():
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E002', '2026-01-01 08:00:00')])
    assert len(sent) == 2


def test_different_employees_independent_gaps():
    sent = _run([r('E001', '2026-01-01 08:00:00'),
                 r('E002', '2026-01-01 08:00:00'),
                 r('E001', '2026-01-01 08:02:00'),  # skipped — within 5min of E001's 08:00
                 r('E002', '2026-01-01 08:06:00'),  # sent — 6min gap for E002
                 ])
    assert len(sent) == 3
    emps = [s['employee_no'] for s in sent]
    assert emps.count('E001') == 1
    assert emps.count('E002') == 2


# --- empty employee_no (exact dedup fallback) ---

def test_empty_emp_exact_duplicate_skipped():
    e = r('', '2026-01-01 08:00:00')
    assert len(_run([e, e])) == 1


def test_empty_emp_different_logtime_both_sent():
    sent = _run([r('', '2026-01-01 08:00:00'),
                 r('', '2026-01-01 08:01:00')])
    assert len(sent) == 2
