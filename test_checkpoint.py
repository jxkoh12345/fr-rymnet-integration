"""Tests for checkpointing + retry. No network: iter_pages, send and
fetch_person_info are stubbed. State dir is redirected to a temp folder.

Run:  python -m unittest test_checkpoint -v
"""
import os
import tempfile
import unittest

import checkpoint
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


class StateTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        checkpoint.STATE_DIR = os.path.join(self._tmp.name, 'state')
        self._orig_send   = main.send
        self._orig_iter   = main.iter_pages
        self._orig_person = main.fetch_person_info
        self._orig_delay  = main.SEND_RETRY_DELAY
        self._orig_maxret = main.MAX_WINDOW_RETRIES
        main.fetch_person_info = lambda pid: {'personCode': f'E_{pid}'}
        main.SEND_RETRY_DELAY = 0
        self.sig = checkpoint.query_signature(START, END, main.DOORS, main.EVENT_TYPE)

    def tearDown(self):
        main.send          = self._orig_send
        main.iter_pages    = self._orig_iter
        main.fetch_person_info = self._orig_person
        main.SEND_RETRY_DELAY  = self._orig_delay
        main.MAX_WINDOW_RETRIES = self._orig_maxret
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

    def test_send_failure_returns_false_and_saves_pending(self):
        main.iter_pages = FakeIterPages(total_pages=4)
        fake = FakeSend(max_success=1)   # batch 1 ok, batch 2 fails
        main.send = fake

        ok, n = self.run_window()

        self.assertFalse(ok)             # regression guard: not None
        self.assertEqual(n, 0)
        self.assertEqual(len(fake.calls), 1 + main.SEND_RETRIES)
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
        self.assertEqual(len(fake.calls), main.SEND_RETRIES)  # only pending retried
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
