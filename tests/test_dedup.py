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

# Dedup key is (employee_no, device) within MIN_GAP_MINUTES (1 min).
# `device` = door name with the IN/OUT direction word stripped, so a turnstile's
# IN and OUT readers collapse to one device (e.g. "WHGF TURN IN 1" and
# "WHGF TURN OUT 1" -> "WHGF TURN 1"). This catches:
#   - turn-back: head-turn triggers the opposite reader on the same gate
#   - double registration: same reader fires twice before the person walks through


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
         patch('main.notify'), \
         patch('main._log_attendance'), \
         patch.object(main.db, 'insert_records', lambda recs: [None] * len(recs)), \
         patch.object(main.db, 'set_status', lambda *a, **k: None), \
         patch('sendlock.send_lock'), \
         patch.object(main, 'FOREIGN_WORKER', False):
        main.run_window('2026-01-01T08:00:00+08:00', '2026-01-01T08:30:00+08:00')

    return sent


def r(emp, logtime, indicator='', location=''):
    return {'employee_no': emp, 'logtime': logtime, 'location': location,
            'indicator': indicator, 'remarks': ''}


# convenience door builders (indicator + location for one physical turnstile)
def IN1(emp, t):  return r(emp, t, 'IN', 'WHGF TURN IN 1')
def OUT1(emp, t): return r(emp, t, 'OUT', 'WHGF TURN OUT 1')
def IN2(emp, t):  return r(emp, t, 'IN', 'WHGF TURN IN 2')


# --- same employee, same device, time-gap window ---

def test_exact_duplicate_skipped():
    assert len(_run([IN1('E001', '2026-01-01 08:00:00'),
                     IN1('E001', '2026-01-01 08:00:00')])) == 1


def test_within_1min_skipped():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:59')])
    assert len(sent) == 1


def test_exactly_1min_sent():
    # boundary: 60s is NOT < 60, so both are sent
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:01:00')])
    assert len(sent) == 2


def test_over_1min_sent():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:02:00')])
    assert len(sent) == 2


def test_third_event_gap_measured_from_last_sent():
    # 08:00 sent, 08:00:30 skipped, 08:01:30 compared against 08:00 → 90s → sent
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:30'),
                 IN1('E001', '2026-01-01 08:01:30')])
    assert len(sent) == 2
    assert sent[1]['logtime'] == '2026-01-01 08:01:30'


# --- turn-back: head-turn triggers the opposite reader on the SAME gate ---

def test_turn_back_same_turnstile_is_duplicate():
    # exits TURN 1, turns head, IN reader on the same gate fires 3s later
    sent = _run([OUT1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:03')])
    assert len(sent) == 1
    assert sent[0]['indicator'] == 'OUT'  # first, genuine event survives


def test_turn_back_at_whcj_fr_doors_is_duplicate():
    # same on the WHCJ facial-recognition doors ("WHCJ IN - FR" / "WHCJ OUT - FR")
    sent = _run([r('E001', '2026-01-01 08:00:00', 'IN', 'WHCJ IN - FR'),
                 r('E001', '2026-01-01 08:00:04', 'OUT', 'WHCJ OUT - FR')])
    assert len(sent) == 1


# --- double registration: same reader fires twice before walking through ---

def test_double_registration_same_reader_is_duplicate():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:02')])
    assert len(sent) == 1


def test_triple_registration_only_first_kept():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:01'),
                 IN1('E001', '2026-01-01 08:00:03')])
    assert len(sent) == 1


# --- genuine events must NOT be suppressed ---

def test_different_turnstile_within_gap_both_sent():
    # assumes the employee does not look at a second gate's device:
    # a hit on a different numbered turnstile is a real, separate event
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN2('E001', '2026-01-01 08:00:10')])
    assert len(sent) == 2


def test_same_turnstile_beyond_gap_both_sent():
    # legit re-entry at the same gate, > 1 min apart
    sent = _run([OUT1('E001', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:02:00')])
    assert len(sent) == 2


# --- different employees are independent ---

def test_different_employees_same_gate_both_sent():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E002', '2026-01-01 08:00:02')])
    assert len(sent) == 2


def test_different_employees_independent_gaps():
    sent = _run([IN1('E001', '2026-01-01 08:00:00'),
                 IN1('E002', '2026-01-01 08:00:00'),
                 IN1('E001', '2026-01-01 08:00:30'),  # skipped — within 1min of E001's 08:00
                 IN1('E002', '2026-01-01 08:01:30'),  # sent — 90s gap for E002
                 ])
    assert len(sent) == 3
    emps = [s['employee_no'] for s in sent]
    assert emps.count('E001') == 1
    assert emps.count('E002') == 2


# --- empty employee_no falls back to exact (emp, logtime) dedup ---

def test_empty_emp_exact_duplicate_skipped():
    e = r('', '2026-01-01 08:00:00')
    assert len(_run([e, e])) == 1


def test_empty_emp_different_logtime_both_sent():
    sent = _run([r('', '2026-01-01 08:00:00'),
                 r('', '2026-01-01 08:00:01')])
    assert len(sent) == 2


# --- full real-world sequence from the reported case (FW-MD586288, 2026-07-09) ---

def test_full_day_sequence_only_turn_back_dropped():
    E = 'MD586288'
    events = [
        r(E, '2026-07-09 06:57:36', 'IN', 'WHGF TURN IN 1'),
        r(E, '2026-07-09 10:02:45', 'OUT', 'WHGF TURN OUT 1'),
        r(E, '2026-07-09 10:10:04', 'IN', 'WHGF TURN IN 1'),
        r(E, '2026-07-09 12:02:11', 'OUT', 'WHGF TURN OUT 1'),
        r(E, '2026-07-09 12:44:26', 'IN', 'WHGF TURN IN 2'),
        r(E, '2026-07-09 13:10:51', 'OUT', 'WHGF TURN OUT 1'),
        r(E, '2026-07-09 13:16:40', 'IN', 'WHGF TURN IN 2'),
        r(E, '2026-07-09 17:03:02', 'OUT', 'WHGF TURN OUT 2'),
        r(E, '2026-07-09 17:29:26', 'IN', 'WHGF TURN IN 2'),
        r(E, '2026-07-09 20:10:27', 'OUT', 'WHGF TURN OUT 1'),
        r(E, '2026-07-09 20:10:30', 'IN', 'WHGF TURN IN 1'),   # turn-back, 3s after the OUT
        r(E, '2026-07-09 20:13:18', 'IN', 'WHGF TURN IN 2'),
        r(E, '2026-07-09 22:02:43', 'OUT', 'WHGF TURN OUT 1'),
    ]
    sent = _run(events)
    assert len(sent) == len(events) - 1
    assert not any(s['logtime'].endswith('20:10:30') for s in sent)
