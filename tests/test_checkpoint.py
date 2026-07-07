"""Tests for checkpointing + retry. No network: iter_pages, send and
fetch_person_info are stubbed. State dir is redirected to a temp folder.

Run:  python -m unittest test_checkpoint -v
"""
import glob
import json
import os
import tempfile
import unittest

import checkpoint
import db
import main

START = '2026-04-01T00:00:00+08:00'
END   = '2026-04-01T00:30:00+08:00'
EVENT_TIME = '2026-04-01T00:10:00+08:00'


def make_event(page, i):
    return {'personId': f'p{page}_{i}', 'doorIndexCode': 0, 'eventTime': EVENT_TIME}


class FakeIterPages:
    """Yields `total_pages` pages of `page_size` events, honoring start_page."""
    def __init__(self, total_pages, page_size=50):
        self.total_pages = total_pages
        self.page_size = page_size
        self.start_page = None

    def __call__(self, **kwargs):
        self.start_page = kwargs.get('start_page', 1)
        return self._gen(self.start_page)

    def _gen(self, start):
        for p in range(start, self.total_pages + 1):
            yield p, [make_event(p, i) for i in range(self.page_size)]


class FakeSend:
    """Records calls. Succeeds `max_success` times, then raises on every call."""
    def __init__(self, max_success=None):
        self.calls = []
        self.successes = 0
        self.max_success = max_success

    def __call__(self, records):
        self.calls.append(list(records))
        if self.max_success is not None and self.successes >= self.max_success:
            raise RuntimeError("boom")
        self.successes += 1
        return {'ok': True}


class FakeSelectiveSend:
    """Whole batches (>1 record) always fail. Single-record sends fail only
    for employee_no in `bad`."""
    def __init__(self, bad):
        self.bad = set(bad)
        self.calls = []

    def __call__(self, records):
        self.calls.append(list(records))
        if len(records) > 1:
            raise RuntimeError("batch rejected")
        if records[0].get('employee_no') in self.bad:
            raise RuntimeError(f"rejected {records[0]['employee_no']}")
        return {'ok': True}


class StateTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        checkpoint.STATE_DIR = os.path.join(self._tmp.name, 'state')
        self._orig_send   = main.send
        self._orig_iter   = main.iter_pages
        self._orig_person = main.fetch_person_info
        self._orig_delay  = main.SEND_RETRY_DELAY
        self._orig_maxret = main.MAX_WINDOW_RETRIES
        self._orig_notify = main.notify
        self._orig_logdir = main.LOG_DIR
        self._orig_db_ins = main.db.insert_records
        self._orig_db_set = main.db.set_status
        main.fetch_person_info = lambda pid: {'personCode': f'E_{pid}'}
        main.SEND_RETRY_DELAY = 0
        main.notify = lambda *a, **k: None   # never hit real Lark in tests
        main.LOG_DIR = os.path.join(self._tmp.name, 'logs')  # don't touch real logs/
        main.db.insert_records = lambda recs: [None] * len(recs)   # never hit real DB
        main.db.set_status = lambda ids, status: None
        self.sig = checkpoint.query_signature(START, END, main.DOORS, main.EVENT_TYPE)

    def tearDown(self):
        main.send          = self._orig_send
        main.iter_pages    = self._orig_iter
        main.fetch_person_info = self._orig_person
        main.SEND_RETRY_DELAY  = self._orig_delay
        main.MAX_WINDOW_RETRIES = self._orig_maxret
        main.notify        = self._orig_notify
        main.LOG_DIR       = self._orig_logdir
        main.db.insert_records = self._orig_db_ins
        main.db.set_status     = self._orig_db_set
        self._tmp.cleanup()

    def run_window(self, reset=False):
        return main.run_window(START, END, reset=reset)


class CheckpointModuleTests(StateTestBase):
    def test_load_missing_returns_zero(self):
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)
        self.assertIsNone(checkpoint.load_pending(self.sig))

    def test_save_and_load_page(self):
        checkpoint.save_page(self.sig, 4)
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 4)

    def test_signature_mismatch_ignored(self):
        checkpoint.save_page(self.sig, 4)
        other = checkpoint.query_signature('X', 'Y', ['1'], 1)
        self.assertEqual(checkpoint.load_checkpoint(other), 0)

    def test_pending_roundtrip_and_clear(self):
        checkpoint.save_pending(self.sig, [3, 4], [{'a': 1}])
        got = checkpoint.load_pending(self.sig)
        self.assertEqual(got['pages'], [3, 4])
        self.assertEqual(got['records'], [{'a': 1}])
        checkpoint.clear_pending(self.sig)
        self.assertIsNone(checkpoint.load_pending(self.sig))

    def test_page_and_pending_independent(self):
        checkpoint.save_page(self.sig, 2)
        checkpoint.save_pending(self.sig, [3, 4], [{'a': 1}])
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 2)  # page survives pending write
        self.assertEqual(checkpoint.load_pending(self.sig)['pages'], [3, 4])

    def test_clear_window_removes_all(self):
        checkpoint.save_page(self.sig, 2)
        checkpoint.save_pending(self.sig, [3], [{'a': 1}])
        checkpoint.clear_window(self.sig)
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)
        self.assertIsNone(checkpoint.load_pending(self.sig))

    def test_reset_wipes_everything(self):
        checkpoint.save_page(self.sig, 2)
        checkpoint.add_failed(START, END)
        checkpoint.reset()
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)
        self.assertEqual(checkpoint.load_failed(), [])

    def test_failed_queue_add_dedups(self):
        checkpoint.add_failed(START, END)
        checkpoint.add_failed(START, END)
        self.assertEqual(len(checkpoint.load_failed()), 1)


class MainFlowTests(StateTestBase):
    def test_happy_path_clears_window_on_success(self):
        # 4 pages x 50 = 200 events -> 2 batches of 100
        main.iter_pages = FakeIterPages(total_pages=4)
        fake = FakeSend()
        main.send = fake

        ok, n = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(n, 200)
        self.assertEqual([len(c) for c in fake.calls], [100, 100])
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)  # cleared
        self.assertIsNone(checkpoint.load_pending(self.sig))

    def test_attendance_staged_to_jsonl_before_send(self):
        main.iter_pages = FakeIterPages(total_pages=2)  # 100 events
        main.send = FakeSend()

        self.run_window()

        files = glob.glob(os.path.join(main.LOG_DIR, 'attendance_*.jsonl'))
        self.assertEqual(len(files), 1)
        with open(files[0], encoding='utf-8') as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 100)                  # one line per record
        rec = json.loads(lines[0])
        self.assertEqual(
            set(rec),
            {'employee_no', 'logtime', 'indicator', 'location', 'remarks', 'duplicate'},
        )
        self.assertFalse(rec['duplicate'])                 # no dupes among 100 distinct people

    def test_attendance_staged_even_when_send_fails(self):
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend(max_success=0)   # every send fails

        self.run_window()

        files = glob.glob(os.path.join(main.LOG_DIR, 'attendance_*.jsonl'))
        self.assertEqual(len(files), 1)
        with open(files[0], encoding='utf-8') as f:
            self.assertEqual(len(f.read().splitlines()), 100)  # staged before the send

    def test_duplicate_records_staged_but_not_sent(self):
        # same person, same logtime, twice -> the 2nd is a duplicate
        main.iter_pages = lambda **kw: iter([(1, [make_event(1, 0), make_event(1, 0)])])
        fake = FakeSend()
        main.send = fake

        ok, n = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(n, 1)                      # duplicate excluded from what's sent
        self.assertEqual(len(fake.calls[0]), 1)

        files = glob.glob(os.path.join(main.LOG_DIR, 'attendance_*.jsonl'))
        with open(files[0], encoding='utf-8') as f:
            logged = [json.loads(line) for line in f.read().splitlines()]
        self.assertEqual(len(logged), 2)             # both staged for audit
        self.assertEqual([r['duplicate'] for r in logged], [False, True])

    def test_send_failure_returns_false_and_saves_pending(self):
        main.iter_pages = FakeIterPages(total_pages=4)
        fake = FakeSend(max_success=1)   # batch 1 ok, batch 2 fails
        main.send = fake

        ok, n = self.run_window()

        self.assertFalse(ok)             # regression guard: not None
        self.assertEqual(n, 0)
        # batch 1 sends once; batch 2 = SEND_RETRIES whole-batch attempts
        # + 100 per-record isolation sends (all fail -> saved pending)
        self.assertEqual(len(fake.calls), 1 + main.SEND_RETRIES + 100)
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 2)
        pending = checkpoint.load_pending(self.sig)
        self.assertEqual(pending['pages'], [3, 4])
        self.assertEqual(len(pending['records']), 100)

    def test_restart_retries_pending_first_no_double_send(self):
        checkpoint.save_page(self.sig, 2)
        checkpoint.save_pending(self.sig, [3, 4], [{'r': i} for i in range(100)])
        iterp = FakeIterPages(total_pages=4)
        main.iter_pages = iterp
        fake = FakeSend()
        main.send = fake

        ok, n = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(len(fake.calls), 1)        # only the pending batch
        self.assertEqual(len(fake.calls[0]), 100)
        self.assertEqual(iterp.start_page, 5)        # fetch resumed past page 4
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)  # cleared on success
        self.assertIsNone(checkpoint.load_pending(self.sig))

    def test_pending_still_failing_stops_before_fetch(self):
        checkpoint.save_page(self.sig, 2)
        checkpoint.save_pending(self.sig, [3, 4], [{'r': i} for i in range(100)])
        iterp = FakeIterPages(total_pages=4)
        main.iter_pages = iterp
        fake = FakeSend(max_success=0)
        main.send = fake

        ok, n = self.run_window()

        self.assertFalse(ok)
        # pending retried whole-batch (SEND_RETRIES) + 100 per-record isolation sends;
        # all fail -> stops before any fetch
        self.assertEqual(len(fake.calls), main.SEND_RETRIES + 100)
        self.assertIsNone(iterp.start_page)                   # never fetched
        self.assertIsNotNone(checkpoint.load_pending(self.sig))
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 2)

    def test_resume_after_crash_completes_and_clears(self):
        # checkpoint at page 4 (crashed mid-fetch); only 4 pages exist -> nothing new
        checkpoint.save_page(self.sig, 4)
        iterp = FakeIterPages(total_pages=4)
        main.iter_pages = iterp
        fake = FakeSend()
        main.send = fake

        ok, n = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(iterp.start_page, 5)
        self.assertEqual(len(fake.calls), 0)
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)  # cleared

    def test_reset_flag_clears_window(self):
        checkpoint.save_page(self.sig, 2)
        iterp = FakeIterPages(total_pages=2)
        main.iter_pages = iterp
        fake = FakeSend()
        main.send = fake

        self.run_window(reset=True)

        self.assertEqual(iterp.start_page, 1)               # ignored the page-2 checkpoint
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)


class WindowRetryTests(StateTestBase):
    def test_retry_recovers_window_and_drains_queue(self):
        checkpoint.add_failed(START, END)
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend()

        recovered = main._retry_failed_windows()

        self.assertEqual(recovered, 100)
        self.assertEqual(checkpoint.load_failed(), [])

    def test_retry_increments_attempts_when_still_failing(self):
        checkpoint.add_failed(START, END)
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend(max_success=0)

        main._retry_failed_windows()

        queue = checkpoint.load_failed()
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]['attempts'], 1)

    def test_retry_gives_up_after_max(self):
        main.MAX_WINDOW_RETRIES = 2
        checkpoint.add_failed(START, END)
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend(max_success=0)

        main._retry_failed_windows()                       # attempt 1
        self.assertEqual(checkpoint.load_failed()[0]['attempts'], 1)
        main._retry_failed_windows()                       # attempt 2 -> give up
        self.assertEqual(checkpoint.load_failed(), [])

    def test_retry_noop_when_empty(self):
        self.assertEqual(main._retry_failed_windows(), 0)


class PerRecordIsolationTests(StateTestBase):
    def test_bad_records_filtered_out_and_good_ones_counted(self):
        # 2 pages x 50 = 100 events -> 1 batch of 100, batch send fails
        main.iter_pages = FakeIterPages(total_pages=2)
        bad = {'E_p1_5', 'E_p2_10'}
        fake = FakeSelectiveSend(bad)
        main.send = fake
        notes: list = []
        main.notify = lambda msg: notes.append(msg)

        orig_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        try:
            ok, n = self.run_window()
            rejected_files = glob.glob(os.path.join('errors', 'rejected_*.json'))
            logged = []
            if rejected_files:
                with open(rejected_files[0], encoding='utf-8') as f:
                    logged = json.load(f)
        finally:
            os.chdir(orig_cwd)

        self.assertTrue(ok)                       # window still completes
        self.assertEqual(n, 100 - len(bad))        # bad ones excluded from sent count
        self.assertEqual(checkpoint.load_checkpoint(self.sig), 0)  # cleared — window fully done
        self.assertIsNone(checkpoint.load_pending(self.sig))

        # isolation actually happened: SEND_RETRIES whole-batch attempts, then
        # every record retried on its own (one send per record).
        batch_calls  = [c for c in fake.calls if len(c) > 1]
        single_calls = [c for c in fake.calls if len(c) == 1]
        self.assertEqual(len(batch_calls), main.SEND_RETRIES)
        self.assertTrue(all(len(c) == 100 for c in batch_calls))
        self.assertEqual(len(single_calls), 100)

        # the good records were delivered individually; the bad ones were attempted too
        attempted = {c[0]['employee_no'] for c in single_calls}
        self.assertEqual(attempted, {f'E_p{p}_{i}' for p in (1, 2) for i in range(50)})

        # bad records recorded individually, full content + window tag preserved
        self.assertEqual(len(rejected_files), 1)
        self.assertEqual(len(logged), len(bad))
        self.assertEqual({r['record']['employee_no'] for r in logged}, bad)
        for entry in logged:
            self.assertEqual(entry['window'], f"{START} → {END}")
            self.assertEqual(
                set(entry['record']),
                {'employee_no', 'logtime', 'indicator', 'location', 'remarks'},
            )

        # a rejection notification fired, naming the rejected file + the resend command
        reject_notes = [m for m in notes if rejected_files[0] in m]
        self.assertEqual(len(reject_notes), 1)
        self.assertIn('debug_pending.py --send', reject_notes[0])

    def test_all_records_bad_saved_pending_not_logged(self):
        main.iter_pages = FakeIterPages(total_pages=2)
        fake = FakeSelectiveSend(bad={f'E_p1_{i}' for i in range(50)} | {f'E_p2_{i}' for i in range(50)})
        main.send = fake
        notes: list = []
        main.notify = lambda msg: notes.append(msg)

        orig_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        try:
            ok, n = self.run_window()
            rejected_files = glob.glob(os.path.join('errors', 'rejected_*.json'))
        finally:
            os.chdir(orig_cwd)

        self.assertFalse(ok)
        self.assertEqual(n, 0)
        self.assertEqual(rejected_files, [])       # nothing logged — saved as pending instead
        self.assertIsNotNone(checkpoint.load_pending(self.sig))

        # notified about the whole-batch rejection, with the isolate command
        self.assertEqual(len(notes), 1)
        self.assertIn('debug_pending.py --send', notes[0])


class DbStatusTests(StateTestBase):
    """hik_records / hik_record_status mirroring (db module stubbed with recorders)."""

    def setUp(self):
        super().setUp()
        self._id = 0
        self.inserted = []      # records passed to insert_records
        self.status_calls = []  # (ids, status)

        def fake_insert(recs):
            self.inserted.extend(recs)
            ids = list(range(self._id + 1, self._id + 1 + len(recs)))
            self._id += len(recs)
            return ids

        main.db.insert_records = fake_insert
        main.db.set_status = lambda ids, status: self.status_calls.append((list(ids), status))

    def test_all_records_inserted_and_success_status_for_sent(self):
        main.iter_pages = FakeIterPages(total_pages=2)  # 100 events, no dups
        main.send = FakeSend()

        ok, n = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(len(self.inserted), 100)                 # every record hits hik_records
        self.assertEqual(self.status_calls, [(list(range(1, 101)), db.STATUS_SUCCESS)])

    def test_duplicates_inserted_but_get_no_status(self):
        # same person twice -> 2nd is dup: both in hik_records, only 1 in status
        main.iter_pages = lambda **kw: iter([(1, [make_event(1, 0), make_event(1, 0)])])
        main.send = FakeSend()

        self.run_window()

        self.assertEqual(len(self.inserted), 2)
        self.assertEqual([r['duplicate'] for r in self.inserted], [False, True])
        self.assertEqual(self.status_calls, [([1], db.STATUS_SUCCESS)])  # dup id 2 has no status

    def test_pending_status_saved_with_ids_then_updated_on_retry(self):
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend(max_success=0)   # everything fails -> pending

        ok, _ = self.run_window()

        self.assertFalse(ok)
        self.assertEqual(self.status_calls, [(list(range(1, 101)), db.STATUS_PENDING)])
        self.assertEqual(checkpoint.load_pending(self.sig)['ids'], list(range(1, 101)))

        # retry succeeds -> same ids move PENDING -> SUCCESS (no re-insert)
        self.status_calls.clear()
        inserted_before = len(self.inserted)
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSend()

        ok, _ = self.run_window()

        self.assertTrue(ok)
        self.assertEqual(self.status_calls[0], (list(range(1, 101)), db.STATUS_SUCCESS))
        self.assertEqual(len(self.inserted), inserted_before)  # pending retry doesn't re-insert

    def test_mixed_isolation_marks_failed_and_success(self):
        main.iter_pages = FakeIterPages(total_pages=2)
        main.send = FakeSelectiveSend({'E_p1_5', 'E_p2_10'})

        orig_cwd = os.getcwd()
        os.chdir(self._tmp.name)   # errors/rejected_*.json goes to temp
        try:
            ok, n = self.run_window()
        finally:
            os.chdir(orig_cwd)

        self.assertTrue(ok)
        by_status = {s: ids for ids, s in self.status_calls}
        self.assertEqual(len(by_status[db.STATUS_FAILED]), 2)
        self.assertEqual(len(by_status[db.STATUS_SUCCESS]), 98)


if __name__ == '__main__':
    unittest.main(verbosity=2)
