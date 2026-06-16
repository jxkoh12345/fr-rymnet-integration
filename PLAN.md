# Plan: Hikvision Data Pipeline Entry Point

## Context
Chain 3 Hikvision APIs into a single pipeline: door events → person info → final submission.
`door_events` returns events with `personId`; `personId` API resolves those to `personCode` (employee_no); `final_data` submits the enriched record.

---

## File Structure

```
hik/
├── main.py                      # entry point
└── signature/
    ├── __init__.py
    ├── auth.py                  # shared Hikvision HMAC-SHA256 auth
    ├── door_events.py           # fetch_all_events() + standalone guard
    ├── personId.py              # fetch_person_info() + standalone guard
    └── final_data.py            # build_body() + send()
```

---

## Implementation Notes

### `signature/auth.py`
- Shared `build_headers(app_key, app_secret, path, body)` used by `door_events` and `personId`
- All Hikvision APIs return `code` as string `"0"` on success — compare with `str(code) == '0'`

### `signature/door_events.py`
- `_fetch_page(params, page_no)` — module-level helper; builds signed request and returns parsed JSON
- `_build_base_params(...)` — strips empty strings/lists from raw param dict
- `iter_events(...)` — **generator**; yields events one by one as each page loads (streaming, not buffered)
  - Chunks `door_index_codes` into groups of 10 (API limit)
  - Fetches page 1 first to get `total` → computes `total_pages`; then fetches pages 2..N lazily
  - Callers receive events as soon as the first page returns, before later pages are fetched
- `fetch_all_events(...)` — kept for backward compat; wraps `list(iter_events(...))`
- Empty strings/lists filtered before sending; `PAGE_SIZE = 50`
- Standalone: `python door_events.py`

### `signature/personId.py`
- `fetch_person_info(person_id: str) -> dict` — returns `data` dict containing `personCode`, `personName`, etc.
- Standalone: `python personId.py <personId>` (defaults to `1203`)

### `signature/final_data.py`
- `URL = 'https://api.rymnet.com/public/attendance/set'`
- Auth: `Authorization: Bearer b441d4cdc71a43dd9e86bd247a9d8a04`
- `build_body(employee_no, logtime, location, indicator='', remarks='') -> dict`
- `send(records: list)` — posts array of up to 100 records; raises on HTTP error
- `indicator` → `DoorList[doorIndexCode]["indicator"]`
- `location` → `DoorList[doorIndexCode]["doorName"]`
- `remarks` → empty string - ""

### `main.py`
- **Producer/consumer pattern** using `threading.Thread` + `queue.Queue(maxsize=500)`
  - **Producer thread**: calls `iter_events`; puts each event into the queue as it arrives; puts `_SENTINEL` when done or on error
  - **Consumer (main thread)**: reads queue, resolves person ID (cached in `person_cache`), builds body, appends to `batch`
  - When `batch` reaches `BATCH_SIZE = 100` → calls `_send_batch()` synchronously; awaits response before continuing
  - After `_SENTINEL` received → flushes remaining `batch` (< 100 rows)
- `maxsize=500` on the queue provides backpressure — producer won't race too far ahead of the consumer
- `person_cache` dict prevents duplicate `personId` API calls within a run
- `_send_batch(batch, batch_num)` logs each record on its own line; `send()` still commented out (testing phase)
- `logtime` reformatted from ISO 8601 → `yyyy-MM-dd HH:mm:ss` via `reformat_time()`
- `START`/`END` configurable via `_start_date`/`_end_date`; falls back to today

---

## Request Body (`final_data`)

```json
{
    "employee_no": "<personCode from personId API>",
    "logtime":     "<eventTime reformatted: yyyy-MM-dd HH:mm:ss>",
    "indicator":   "",
    "location":    "<doorName from door_events>",
    "remarks":     ""
}
```

---

## Unresolved
- Uncomment `send()` when ready to go live
