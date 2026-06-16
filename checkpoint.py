"""Disk-backed state for resumable, retryable sync.

Per-window state, so multiple failed windows can coexist and be retried
independently:

  state/windows/<sig_hash>.json  -> { "query": {...}, "page": N, "pending": {...}|null }
  state/failed.json              -> [ { "start", "end", "attempts" }, ... ]

`page`    = last fully-sent page for that window.
`pending` = the batch that failed to send (retried first when the window reruns).
`failed`  = queue of windows the scheduler must retry on later ticks.

State is scoped to a query signature (date range + doors + event type). On full
success a window's file is deleted (it never needs to rerun).
"""
import hashlib
import json
import os
import shutil

STATE_DIR = 'state'


def _windows_dir() -> str:
    return os.path.join(STATE_DIR, 'windows')


def _failed_path() -> str:
    return os.path.join(STATE_DIR, 'failed.json')


def query_signature(start: str, end: str, doors: list, event_type: int) -> dict:
    """Identity of a window. State only resumes when this matches."""
    return {
        'start': start,
        'end': end,
        'doors': sorted(str(d) for d in doors),
        'event_type': event_type,
    }


def _window_path(signature: dict) -> str:
    raw = json.dumps(signature, sort_keys=True)
    h = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return os.path.join(_windows_dir(), f'{h}.json')


def _load(signature: dict) -> dict:
    try:
        with open(_window_path(signature), encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {'page': 0, 'pending': None}
    if data.get('query') != signature:  # stale file / hash collision
        return {'page': 0, 'pending': None}
    return {'page': data.get('page', 0), 'pending': data.get('pending')}


def _save(signature: dict, page: int, pending):
    os.makedirs(_windows_dir(), exist_ok=True)
    with open(_window_path(signature), 'w', encoding='utf-8') as f:
        json.dump({'query': signature, 'page': page, 'pending': pending}, f)


def load_checkpoint(signature: dict) -> int:
    return _load(signature)['page']


def save_page(signature: dict, page: int):
    _save(signature, page, _load(signature)['pending'])


def load_pending(signature: dict):
    """The unsent batch {pages, records} for this window, or None."""
    return _load(signature)['pending']


def save_pending(signature: dict, pages: list, records: list):
    _save(signature, _load(signature)['page'], {'pages': pages, 'records': records})


def clear_pending(signature: dict):
    _save(signature, _load(signature)['page'], None)


def clear_window(signature: dict):
    """Delete a window's state entirely (used on full success)."""
    try:
        os.remove(_window_path(signature))
    except FileNotFoundError:
        pass


# --- failed-window queue ---

def load_failed() -> list:
    try:
        with open(_failed_path(), encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_failed(items: list):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(_failed_path(), 'w', encoding='utf-8') as f:
        json.dump(items, f)


def add_failed(start: str, end: str):
    items = load_failed()
    if any(it['start'] == start and it['end'] == end for it in items):
        return
    items.append({'start': start, 'end': end, 'attempts': 0})
    save_failed(items)


def reset():
    """Wipe all saved state (used by --reset)."""
    try:
        shutil.rmtree(STATE_DIR)
    except FileNotFoundError:
        pass
