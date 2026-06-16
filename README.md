# HIK → Rymnet Attendance Sync

Continuously pulls door-access events from a Hikvision (Artemis) access-control
API, resolves each person's employee code, and pushes attendance records to the
Rymnet attendance API. Runs as a continuous 30-minute-window scheduler with
per-window checkpointing, automatic retries, and Lark (Feishu) failure
notifications.

---

## Table of contents

- [How it works](#how-it-works)
- [Data flow](#data-flow)
- [Reliability model](#reliability-model)
- [Project layout](#project-layout)
- [Configuration (`.env`)](#configuration-env)
- [Module & function reference](#module--function-reference)
- [Running locally / testing](#running-locally--testing)
- [Running under systemd](#running-under-systemd)
- [Operations & troubleshooting](#operations--troubleshooting)
- [Tests](#tests)

---

## How it works

The app processes time in **30-minute windows** (48 per day). At each half-hour
boundary it fetches the window that just *completed* and ships it:

```
08:30 tick → fetch events for 08:00:00 → 08:30:00 → send to Rymnet
09:00 tick → fetch events for 08:30:00 → 09:00:00 → send to Rymnet
...
```

Within a window, events are fetched page-by-page (50/page) from Hikvision,
grouped into **page-aligned batches** of up to 100 records, and POSTed to
Rymnet. Each successfully sent batch advances a per-window page checkpoint, so a
crash or outage never re-sends already-delivered records.

## Data flow

```
Hikvision door/events API          (signature/door_events.iter_pages)
        │  pages of raw events
        ▼
  per-event resolve                 (main._resolve_record)
   • personId → personCode          (signature/personId.fetch_person_info, cached)
   • doorIndexCode → name/indicator (DoorList)
   • eventTime → "YYYY-MM-DD HH:MM:SS"
        │  Rymnet record bodies      (signature/final_data.build_body)
        ▼
  page-aligned batches (≤100)
        │
        ▼
   Rymnet attendance API            (signature/final_data.send)
```

Failures at any stage are checkpointed to `state/` and surfaced to Lark
(`notifier.notify`).

## Reliability model

Three independent layers:

| Layer | Mechanism | On failure |
|-------|-----------|------------|
| **Batch send** | `SEND_RETRIES` (3) inline attempts with `SEND_RETRY_DELAY` (2s) backoff | Batch saved to the window's `pending`; window marked failed |
| **Window** | Per-window page checkpoint + pending batch in `state/windows/<hash>.json` | Window added to `state/failed.json`; retried one per scheduler tick up to `MAX_WINDOW_RETRIES` (10) |
| **Give-up** | After `MAX_WINDOW_RETRIES` ticks | Window dropped from queue, **Lark alert** for manual intervention (state file left intact for manual rerun) |

A window's state file is **deleted on full success**, so `state/` stays small.

**Known gaps (not handled):**
- **Missed windows during downtime** — `_next_window()` is forward-only. If the
  host is down across boundaries, those windows are never fetched (no catch-up).
- **In-memory daily total** — a crash mid-day resets the running tally, so the
  midnight summary undercounts after a restart.

## Project layout

```
hik/
├── main.py                 # entry point: scheduler, window processing, CLI
├── checkpoint.py           # disk-backed per-window state + failed-window queue
├── notifier.py             # Lark (Feishu) notifications
├── DoorList.py             # door metadata (id → type/name/indicator)
├── signature/
│   ├── auth.py             # Hikvision HMAC request signing
│   ├── door_events.py      # door-event fetch (paged / resumable)
│   ├── personId.py         # person info lookup
│   ├── final_data.py       # Rymnet record builder + sender
│   └── generate.py         # standalone util: dump door list to CSV (not used by app)
├── test_checkpoint.py      # checkpoint + retry tests (no network)
├── test_lark.py            # live Lark smoke test
├── .env                    # secrets / endpoints (gitignored)
└── state/                  # runtime checkpoints (gitignored, auto-created)
```

## Configuration (`.env`)

All secrets and endpoints live in `.env` (gitignored). Required keys:

```ini
# Hikvision
HIK_APP_KEY=...
HIK_APP_SECRET=...
HIK_BASE_URL=https://10.1.74.105
HIK_DOOR_EVENTS_PATH=/artemis/api/acs/v1/door/events
HIK_PERSON_PATH=/artemis/api/resource/v1/person/personId/personInfo

# Rymnet
RYMNET_URL=https://api.rymnet.com/public/attendance/set
RYMNET_TOKEN=...

# Lark (optional — if any is blank, notifications are skipped, no crash)
LARK_APP_ID=cli_...
LARK_APP_SECRET=...
LARK_UNION_ID=on_...
```

Tunable constants live at the top of `main.py`: `BATCH_SIZE`, `SEND_RETRIES`,
`SEND_RETRY_DELAY`, `WINDOW_MINUTES`, `MAX_WINDOW_RETRIES`, `TIMEZONE`,
`EVENT_TYPE`.

---

## Module & function reference

### `main.py` — orchestration & entry point

| Function | Signature | Description |
|----------|-----------|-------------|
| `_fmt` | `(dt) -> str` | Format a `datetime` as ISO 8601 with the configured `TIMEZONE` suffix. |
| `reformat_time` | `(iso_str) -> str` | Convert a Hikvision ISO timestamp to Rymnet's `YYYY-MM-DD HH:MM:SS`. |
| `_send_with_retry` | `(records, label) -> (bool, float)` | POST a batch via `final_data.send`, retrying up to `SEND_RETRIES` with `SEND_RETRY_DELAY` backoff. Returns `(success, elapsed_seconds)`. |
| `_resolve_record` | `(item, person_cache) -> dict` | Turn one raw door event into a Rymnet body: resolves `personId`→`personCode` (cached, blank on lookup error), maps `doorIndexCode` via `DoorList`, reformats time. |
| `run_window` | `(start, end, reset=False) -> (bool, int)` | Process one window end-to-end: retry any pending batch, resume from the last checkpointed page, fetch→build→batch→send, checkpoint per batch. Clears window state on success. Returns `(ok, records_sent)`; `ok=False` if it did not complete. |
| `_next_window` | `() -> (datetime, datetime)` | Compute the `(start, end)` of the most recently completed 30-minute window relative to `now`. |
| `_retry_failed_windows` | `() -> int` | Retry every window in `state/failed.json` once; recovered ones drain from the queue, others increment `attempts`; gives up + alerts after `MAX_WINDOW_RETRIES`. Returns records recovered. |
| `scheduler` | `(reset=False) -> None` | Infinite loop: sleep to the next boundary → retry failed windows → process the current window → at midnight send the daily summary. `reset=True` wipes all state once at start. |

**CLI (`__main__`):**
- `--reset` — clear saved state before starting.
- `--start DATETIME --end DATETIME` — **test mode**: process a single explicit
  window instead of starting the scheduler (timezone auto-appended if omitted).

### `checkpoint.py` — durable per-window state

State files (under `STATE_DIR`, default `state/`):
- `state/windows/<sig_hash>.json` → `{query, page, pending}` per window
- `state/failed.json` → `[{start, end, attempts}, ...]`

| Function | Signature | Description |
|----------|-----------|-------------|
| `query_signature` | `(start, end, doors, event_type) -> dict` | Identity of a window. State only resumes when this matches (so changing dates/doors invalidates old state). |
| `load_checkpoint` | `(signature) -> int` | Last fully-sent page for the window, `0` if none. |
| `save_page` | `(signature, page)` | Persist the last-sent page (preserves existing `pending`). |
| `load_pending` | `(signature) -> dict\|None` | The unsent batch `{pages, records}`, or `None`. |
| `save_pending` | `(signature, pages, records)` | Persist a failed batch for later retry (preserves `page`). |
| `clear_pending` | `(signature)` | Drop the pending batch (after it sends). |
| `clear_window` | `(signature)` | Delete the window's state file entirely (on full success). |
| `load_failed` / `save_failed` | `() -> list` / `(items)` | Read/write the failed-window queue. |
| `add_failed` | `(start, end)` | Enqueue a window for retry (deduplicated). |
| `reset` | `()` | Wipe the entire `state/` directory. |

### `signature/door_events.py` — Hikvision event fetch

| Function | Signature | Description |
|----------|-----------|-------------|
| `_fetch_page` | `(params, page_no) -> dict` | POST one signed page request to the door/events endpoint. |
| `_build_base_params` | `(...) -> dict` | Assemble query params, dropping empty values. |
| `iter_pages` | `(..., start_page=1)` → yields `(page_no, events_list)` | **Used by the app.** Resumable paging for a single door chunk (≤10 doors). Computes total pages from the first fetched page so it can resume mid-run. Rejects >10 doors. |
| `iter_events` | `(...)` → yields events | Legacy: yields individual events across all door chunks (no resume). |
| `fetch_all_events` | `(...) -> list` | Eagerly collect all events into a list. |

`PAGE_SIZE = 50`. Credentials/URL loaded from `.env`. TLS verification is
disabled (`verify=False`) for the self-signed Hikvision host; the urllib3
warning is suppressed.

### `signature/personId.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `fetch_person_info` | `(person_id) -> dict` | Look up a person by `personId`; returns the `data` object (contains `personCode`). Raises `RuntimeError` on API error. |

### `signature/final_data.py` — Rymnet output

| Function | Signature | Description |
|----------|-----------|-------------|
| `build_body` | `(employee_no, logtime, location, indicator='', remarks='') -> dict` | Build a single Rymnet attendance record. |
| `send` | `(records) -> dict` | POST a list of records (one batch) to Rymnet with the bearer token. Raises on non-2xx (`raise_for_status`). |

### `signature/auth.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `build_headers` | `(app_key, app_secret, path, body) -> dict` | Build Hikvision Artemis HMAC-SHA256 signed headers (`x-ca-*`, Content-MD5). |

### `notifier.py` — Lark notifications

| Function | Signature | Description |
|----------|-----------|-------------|
| `_get_token` | `() -> str` | Fetch a Lark `tenant_access_token` (custom-app internal auth). |
| `notify` | `(message) -> None` | Send a text message to `LARK_UNION_ID`. No-op (warning logged) if Lark env vars are unset; never raises. |

### `DoorList.py`

`DoorList` — dict mapping door id → `{type, doorName, indicator}`. `main.DOORS`
is derived from entries where `type == 'Door'`. Used to enrich records with
location name and IN/OUT indicator.

---

## Running locally / testing

Dependencies are managed with **uv** (`pyproject.toml` / `uv.lock`).

```bash
uv sync                      # create .venv and install deps
```

Process a single explicit window (does not start the scheduler):

```bash
uv run python main.py --start 2026-04-01T08:00:00 --end 2026-04-01T08:30:00
```

Start the continuous scheduler:

```bash
uv run python main.py            # resume from any saved state
uv run python main.py --reset    # wipe state, then run
```

Smoke-test Lark:

```bash
uv run python test_lark.py       # sends one message to LARK_UNION_ID
```

---

## Running under systemd

### 1. Deploy code + dependencies

```bash
sudo mkdir -p /opt/hik
sudo chown $USER:$USER /opt/hik
# copy the project to /opt/hik (git clone / scp / rsync), then:
cd /opt/hik

curl -LsSf https://astral.sh/uv/install.sh | sh   # if uv not installed
uv sync                                            # creates /opt/hik/.venv
ls /opt/hik/.venv/bin/python                        # confirm interpreter
```

### 2. Create `.env` on the host

`.env` is gitignored and won't ship with the repo — create it manually:

```bash
nano /opt/hik/.env       # paste HIK_*, RYMNET_*, LARK_* values
chmod 600 /opt/hik/.env  # restrict secrets to owner
```

### 3. Verify manually before daemonizing

```bash
cd /opt/hik
.venv/bin/python main.py --start 2026-04-01T08:00:00 --end 2026-04-01T08:30:00
```

Confirm it fetches, sends, and (if configured) delivers a Lark message.

### 4. Create a service user

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin hik
sudo chown -R hik:hik /opt/hik    # needs write access to state/
```

### 5. Create the unit file

`sudo nano /etc/systemd/system/hik-sync.service`

```ini
[Unit]
Description=HIK -> Rymnet attendance sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hik
WorkingDirectory=/opt/hik
ExecStart=/opt/hik/.venv/bin/python main.py
Restart=always
RestartSec=10
Environment=TZ=Asia/Kuala_Lumpur

[Install]
WantedBy=multi-user.target
```

> **`WorkingDirectory` is required** — `.env`, `state/`, and all relative paths
> resolve from the current directory.
>
> **Timezone matters** — the code hardcodes the `+08:00` offset and
> `_next_window()` uses local time. Set `TZ` (and the host clock) to a matching
> zone or windows will be offset from the wall clock.

### 6. Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hik-sync
```

### 7. View logs

Logs go to stdout → journald (there is no log file):

```bash
journalctl -u hik-sync -f            # live tail
journalctl -u hik-sync --since today
journalctl --disk-usage              # check journal size
```

---

## Operations & troubleshooting

```bash
sudo systemctl status hik-sync       # state + recent logs
sudo systemctl restart hik-sync
sudo systemctl stop hik-sync

# Full state reset (stop first):
sudo systemctl stop hik-sync
sudo -u hik rm -rf /opt/hik/state
sudo systemctl start hik-sync

# Manually re-run a specific failed window (resumes from its checkpoint):
sudo -u hik /opt/hik/.venv/bin/python /opt/hik/main.py \
     --start 2026-04-01T08:00:00 --end 2026-04-01T08:30:00
```

**Failure notifications** (sent to Lark): pending batch still failing, Rymnet
send failed after retries, Hikvision fetch error, gave-up windows, and the
daily summary at midnight.

**State inspection:**
```bash
cat /opt/hik/state/failed.json                 # windows awaiting retry
ls  /opt/hik/state/windows/                     # in-progress / failed windows
```

---

## Tests

`test_checkpoint.py` covers checkpointing and the retry logic with all network
calls stubbed (no live APIs) and a temp state directory:

```bash
uv run python -m unittest test_checkpoint -v
```

Covers: page-aligned batching, send-failure → pending save → `(False, 0)`
return, resume-after-crash, pending-retry-first (no double-send), per-window
isolation, window-retry recovery, attempt counting, and give-up after
`MAX_WINDOW_RETRIES`.
