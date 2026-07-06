"""Cross-process lock so only one process sends to Rymnet at a time.

The scheduler daemon and any manual tool (debug_pending, main --start/--end,
--recover-windows) each throttle their own sends to 10/min, but their windows
are independent — run together they can still blow Rymnet's quota. This lock
serializes send bursts across processes: a second sender blocks until the first
finishes. Held only during a send burst, so the daemon releases it while it
sleeps between windows. Auto-released on process exit, including a crash.
"""
import contextlib
import functools
import os

_LOCK_PATH = os.path.join(os.path.dirname(__file__), 'state', 'rymnet_send.lock')

try:
    import fcntl

    def _acquire(f):
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def _release(f):
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
except ImportError:  # Windows dev
    import time
    import msvcrt

    def _acquire(f):
        while True:
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
                return
            except OSError:
                time.sleep(0.5)

    def _release(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)


@contextlib.contextmanager
def send_lock():
    """Block until this process holds the exclusive Rymnet send lock."""
    os.makedirs(os.path.dirname(_LOCK_PATH), exist_ok=True)
    f = open(_LOCK_PATH, 'w')
    try:
        _acquire(f)
        yield
    finally:
        _release(f)
        f.close()


def locked(fn):
    """Decorator: hold send_lock() for the whole call."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with send_lock():
            return fn(*args, **kwargs)
    return wrapper
