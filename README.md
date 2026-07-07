# HIK → Rymnet Attendance Sync

Continuously pulls door-access events from a Hikvision (Artemis) access-control
API, resolves each person's employee code, records everything to Postgres, and
pushes attendance records to the Rymnet attendance API. Runs as a continuous
30-minute-window scheduler with per-window checkpointing, automatic retries,
per-record poison isolation, cross-process send locking, Rymnet rate limiting,
and Lark (Feishu) notifications.

---

## Table of contents

- [How it works](#how-it-works)
- [Data flow](#data-flow)
- [Database](#database)
- [Reliability model](#reliability-model)
- [Commands](#commands)
- [Common operational flows](#common-operational-flows)
- [Configuration (`.env`)](#configuration-env)
- [Project layout](#project-layout)
- [Module reference](#module-reference)
- [Running under systemd](#running-under-systemd)
- [Tests](#tests)

---

## How it works

Time is processed in **30-minute windows** (48 per day). At each half-hour
boundary the scheduler fetches the window that just *completed* and ships it:

```
08:30 tick → fetch events for 08:00:00 → 08:30:00 → record to DB → send to Rymnet
09:00 tick → fetch events for 08:30:00 → 09:00:00 → record to DB → send to Rymnet
...
```

Within a window, events are fetched page-by-page (50/page) from Hikvision,
every record is inserted into `hik_records` (full audit trail, duplicates
included), non-duplicates are grouped into **page-aligned batches** of up to 100
records and POSTed to Rymnet. Each successfully sent batch advances a per-window
page checkpoint, so a crash or outage never re-sends already-delivered records.

## Data flow

```
Hikvision door/events API              (signature/door_events.iter_pages)
        │  pages of raw events (50/page)
        ▼
  resolve + normalize                  (main._prepare_page → _resolve_record)
   • personId → personCode             (signature/personId.fetch_person_info, cached)
   • doorIndexCode → name/indicator    (DoorList)
   • eventTime → "YYYY-MM-DD HH:MM:SS"
   • FW… → FW-… prefix normalization
   • mark duplicates (<5 min gap same person)
   • optional FOREIGN_WORKER filter
        │
        ├─► hik_records  (ALL records, incl. duplicates — audit)   (db.insert_records)
        │
        ▼  non-duplicate records only
  page-aligned batches (≤100)
        │
        ▼
   Rymnet attendance API               (signature/final_data.send)
   • rate-limited: 10 calls / 60s      (final_data._throttle)
   • serialized across processes        (sendlock.send_lock)
        │
        ▼
  hik_record_status  ← SUCCESS / PENDING / FAILED per record  (db.set_status)
```

## Database

Postgres, two tables. The DB layer is **best-effort**: if `PG_*` env vars are
unset, every DB call is a silent no-op; DB errors are logged, never raised — the
sync keeps working without the DB.

### `hik_records` — full audit trail

Every fetched record, **duplicates included**. One row per record seen, ever.
Re-running a window inserts *fresh* rows (never upserts), so reruns always add
new entries.

| column | type | notes |
|--------|------|-------|
| `id` | `uuid` | PK, DB-generated (`gen_random_uuid()`) |
| `employee_no` | varchar | |
| `logtime` | timestamp | `YYYY-MM-DD HH:MM:SS` |
| `location`, `indicator`, `remark` | varchar | |
| `date_created`, `created_by` | timestamp / varchar | `created_by='system'` on insert |
| `date_modified`, `modified_by`, `date_deleted`, `deleted_by` | | audit fields |

### `hik_record_status` — send status

One row per record that was actually **ready to send** (non-duplicates only —
duplicates get no status row). Tracks the send lifecycle.

| column | type | notes |
|--------|------|-------|
| `id` | `uuid` | PK, DB-generated |
| `hik_record_id` | `uuid` | FK → `hik_records.id` `ON DELETE CASCADE` |
| `status` | varchar | `SUCCESS` \| `PENDING` \| `FAILED` |
| `date_created`, `created_by` | | `created_by='system'` on first insert |
| `date_modified`, `modified_by` | | `modified_by=<OS user>` on manual reruns |

**Status lifecycle:** first write inserts (`created_by='system'`); later
transitions update in place (`modified_by=ACTOR`).

- `SUCCESS` — delivered to Rymnet.
- `PENDING` — queued / awaiting retry (whole batch failed, saved for later).
- `FAILED` — isolated as a poison record and dropped, or gave up after max retries.

`db.ACTOR` is `'system'` for the scheduler; manual reruns (`main.py --start/--end`,
`--recover-windows`, `debug_pending.py`) set it to the OS username so
`modified_by` reflects the human operator.

### Schema / migration

`schema.sql` holds the incremental migration for the `status` column + a
supporting index (idempotent, `IF NOT EXISTS`). Apply with:

```bash
psql "$PG_CONN" -f schema.sql
```

---

## Reliability model

Layered, each independent:

| Layer | Mechanism | On failure |
|-------|-----------|------------|
| **Batch send** | `SEND_RETRIES` (3) inline attempts, `SEND_RETRY_DELAY` (2s) backoff | fall through to per-record isolation |
| **Per-record isolation** | `_send_resilient` re-sends each record of a failed batch alone | good records delivered; **poison records** logged to `errors/`, marked `FAILED`, dropped; window continues |
| **Total batch failure** | if *no* record sends alone (e.g. Rymnet outage) | batch saved as `pending`, records marked `PENDING`, window stops (returns `(False,0)`) |
| **Window** | per-window page checkpoint + pending batch in `state/windows/<hash>.json` | scheduler re-queues to `state/failed.json`, retried one/tick up to `MAX_WINDOW_RETRIES` (10) |
| **Give-up** | after `MAX_WINDOW_RETRIES` ticks | window dropped from queue, stuck records marked `FAILED`, **Lark alert** for manual intervention |
| **Rate limit** | `final_data._throttle` — sliding window, 10 calls / 60s | blocks (waits) instead of getting HTTP 429 |
| **Cross-process lock** | `sendlock` — `fcntl` file lock on `state/rymnet_send.lock` | a second sender **blocks** until the first finishes; prevents scheduler + manual tool both sending at once |

A window's state file is **deleted on full success**, so `state/windows/` stays
small. On every successful window the scheduler also sends a Lark "window done"
notification.

**Known gaps (not handled):**
- **Missed windows during downtime** — `_next_window()` is forward-only; windows
  missed while the host is down are not auto-caught-up (use a manual `--start/--end`
  backfill).
- **In-memory daily total** — a crash mid-day resets the running tally, so the
  midnight summary undercounts after a restart.
- **Cross-process rate window** — the 10/min limiter is per-process; the send
  lock serializes bursts so they can't overlap, but two processes' throttle
  windows are still independent. Don't run manual send tools while the scheduler
  is actively sending (the lock will make you wait anyway).

---

## Commands

Dependencies are managed with **uv** (`pyproject.toml` / `uv.lock`).

```bash
uv sync                          # create .venv and install deps
```

### Scheduler (production)

```bash
uv run main.py                   # continuous scheduler, resume from saved state
uv run main.py --reset           # wipe ALL state, then run (scheduler mode only)
```

### Manual window — fetch + send to Rymnet

Process one explicit window instead of the scheduler. Inserts to DB **and**
sends to Rymnet. Audits `modified_by` as the OS user.

```bash
uv run main.py --start 2026-06-26T00:00:00 --end 2026-07-03T00:00:00
```

Timezone (`+08:00`) is auto-appended if omitted. Resumes from the window's
checkpoint if interrupted.

### Backfill `hik_records` only — NO send

Fetch a window and insert to `hik_records` for audit **without** sending to
Rymnet. `hik_record_status` is filled later (next scheduler window, or a manual
`main.py` rerun). Reruns always add fresh rows.

```bash
uv run fill_records.py --start 2026-06-26T00:00:00 --end 2026-07-03T00:00:00
```

### Backfill into the scratch test table

Same as above but targets `hik_records_test` (a clone of `hik_records`) — for
testing, drop it when done.

```bash
uv run fill_records_test.py --start 2026-06-26T00:00:00 --end 2026-07-03T00:00:00
```

### List window state files

Filenames are signature hashes — this prints each window's range / page /
pending status so you can find the one you want.

```bash
uv run list_windows.py
# 0961….json: 2026-06-19T08:00:00+08:00 -> …08:30:00+08:00  page=2  pending=YES
```

### Recover stuck / orphan windows

Re-run every orphan window in `state/windows/` (resume from its checkpoint),
then exit. Use after an outage to flush everything that didn't complete.

```bash
uv run main.py --recover-windows
```

### Isolate a poison record (`debug_pending.py`)

Rymnet's endpoint is all-or-nothing: one bad record 500s the whole batch with a
generic message. This finds exactly which record(s) Rymnet rejects.

```bash
uv run debug_pending.py                    # inspect pending batches, no API calls
uv run debug_pending.py --send             # isolate: send each alone, drop+log bad ones, unstick windows
uv run debug_pending.py --send FILE.json   # ad-hoc: send records from a JSON file (e.g. an errors/ file)
```

With `--send`, good records are delivered (Rymnet dedupes), rejected ones are
logged to `errors/` and dropped, and the window's pending batch is cleared +
checkpoint advanced so the scheduler resumes past it.

### Clear window state

```bash
uv run main.py --clear-windows   # delete all state/windows/ files, then exit
rm -f state/failed.json          # clear the retry queue (separate)
```

### Query raw Hikvision events (`find_username.py`)

```bash
# all events in a time range → find_results.log
uv run find_username.py -t 2026-06-22T08:00:00 2026-06-22T09:00:00
# filter by employee name or ID (partial, case-insensitive)
uv run find_username.py -t 2026-06-22T08:00:00 2026-06-22T09:00:00 -u john
```

Each run overwrites `find_results.log` with full JSON; each event carries a
`_resolved` field (personCode/personName). Terminal prints a one-line summary.

### Smoke-test Lark

```bash
uv run python tests/test_lark.py
```

---

## Common operational flows

### Resend a range Rymnet deleted

Rymnet deleted data for a range and you need to re-push it cleanly:

```bash
# 1. stop the scheduler daemon (so it doesn't retry mid-backfill)
sudo systemctl stop hik-sync

# 2. clear stale 30-min state so nothing re-sends after restart (avoids duplicates)
uv run main.py --clear-windows
rm -f state/failed.json

# 3. backfill + send the exact deleted range — run under tmux/nohup, it's long
uv run main.py --start 2026-06-26T00:00:00 --end 2026-07-03T00:00:00

# 4. confirm it finished (window file auto-deletes on full success)
uv run list_windows.py

# 5. restart the scheduler
sudo systemctl start hik-sync
```

> Match the range to **exactly** what Rymnet deleted. Resending records Rymnet
> did *not* delete risks double-counting (Rymnet-side dedupe is the only guard).

### A window keeps failing

1. `uv run list_windows.py` → find the window with `pending=YES`.
2. `uv run debug_pending.py` → inspect it for obvious problems.
3. `uv run debug_pending.py --send` → isolate: delivers the good records, drops
   the poison one(s) to `errors/`, and unsticks the window.

### HTTP 429 during a manual isolation run

That's the Rymnet rate limit (10/min). The rate limiter now paces sends
automatically, so a large isolation run just takes longer (~10/min) instead of
429'ing. If you still see 429s, another process is sending concurrently — stop
the scheduler first.

---

## Configuration (`.env`)

All secrets and endpoints live in `.env` (gitignored):

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

# Postgres (attendance DB) — leave blank to disable the DB sink entirely
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=hik_rymnet
PG_USER=postgres
PG_PASSWORD=...

# Filter: true = only send records whose employee_no starts with "FW"
FOREIGN_WORKER=false

# Optional: notify to Lark when this employee_no is seen (debugging)
EVENT_TEST=

# Lark (optional — if any is blank, notifications are skipped, no crash)
LARK_APP_ID=cli_...
LARK_APP_SECRET=...
LARK_UNION_ID=on_...
```

> **All `PG_*` with a value are used** — if `PG_HOST` and `PG_DATABASE` are set,
> the DB sink is enabled. Leaving them blank disables it (sync still runs).
> Note: an empty value is dropped from the connection, so if the DB needs a
> password, `PG_PASSWORD` **must** be present.

Tunable constants at the top of `main.py`: `BATCH_SIZE` (100), `SEND_RETRIES`
(3), `SEND_RETRY_DELAY` (2s), `WINDOW_MINUTES` (30), `MAX_WINDOW_RETRIES` (10),
`MIN_GAP_MINUTES` (5), `TIMEZONE`, `EVENT_TYPE`. Rate limit in
`signature/final_data.py`: `RATE_LIMIT` (10), `RATE_WINDOW` (60s).

---

## Project layout

```
hik/
├── main.py                 # entry point: scheduler, window processing, CLI
├── db.py                   # Postgres sink: insert_records, set_status (best-effort)
├── checkpoint.py           # disk-backed per-window state + failed-window queue
├── sendlock.py             # cross-process Rymnet send lock (fcntl / msvcrt)
├── notifier.py             # Lark (Feishu) notifications
├── DoorList.py             # door metadata (id → type/name/indicator)
├── fill_records.py         # backfill hik_records for a window (no send, no state)
├── fill_records_test.py    # same, into scratch table hik_records_test
├── debug_pending.py        # isolate which pending record(s) Rymnet rejects
├── list_windows.py         # print state/windows/ files: range / page / pending
├── find_username.py        # CLI: query raw Hikvision events by time / employee
├── schema.sql              # incremental migration (status column + index)
├── signature/
│   ├── auth.py             # Hikvision HMAC request signing
│   ├── door_events.py      # door-event fetch (paged / resumable)
│   ├── personId.py         # person info lookup
│   ├── final_data.py       # Rymnet record builder + sender + rate limiter
│   └── generate.py         # standalone util: dump door list to CSV
├── tests/                  # pytest suite (network stubbed)
├── .env                    # secrets / endpoints (gitignored)
├── logs/                   # attendance_<date>.jsonl staging (gitignored)
├── errors/                 # errors.log + rejected/bad records (gitignored)
└── state/                  # runtime checkpoints + send lock (gitignored)
```

---

## Module reference

### `main.py`

| Function | Description |
|----------|-------------|
| `run_window(start, end, reset=False) -> (ok, sent)` | Process one window end-to-end: retry pending batch, resume from checkpoint, fetch→resolve→audit→DB→batch→send. **Holds the cross-process send lock** for the whole call. Clears state on success + sends "window done" Lark note. |
| `_prepare_page(events, person_cache, seen, last_sent) -> (audited, deduped, dupes)` | Resolve a page, normalize `FW` prefixes, mark duplicates (<`MIN_GAP_MINUTES` same person), honor `FOREIGN_WORKER`. `audited`=all (audit), `deduped`=records to send. |
| `_resolve_record(item, person_cache) -> dict` | One raw event → Rymnet body: personId→personCode (cached), door→name/indicator, time reformat. |
| `_send_resilient(records, pages, sig, window, ids) -> (stop, sent)` | Send a batch; on failure isolate per-record. Mirrors outcomes to `hik_record_status`. |
| `_send_with_retry(records, label) -> (ok, secs)` | POST a batch, `SEND_RETRIES` attempts with backoff. |
| `_retry_failed_windows() -> int` | Retry each `state/failed.json` window once; give up + alert after `MAX_WINDOW_RETRIES`. |
| `recover_windows() -> int` | Re-run orphan windows (files present, not in failed queue). |
| `scheduler(reset=False)` | Infinite loop: sleep to boundary → retry failed → process window → midnight summary. |

**CLI:** `--reset`, `--clear-windows`, `--recover-windows`, `--start/--end`.

### `db.py`

| Function | Description |
|----------|-------------|
| `enabled() -> bool` | True if `PG_HOST` and `PG_DATABASE` are set. |
| `insert_records(records) -> list` | Insert every record into `hik_records`; returns new UUIDs aligned with input (`[None]*n` if disabled/failed). |
| `set_status(record_ids, status)` | Upsert `hik_record_status`: first write inserts, later transitions update (`modified_by=ACTOR`). Skips `None` ids. |

### `checkpoint.py`

State: `state/windows/<sig_hash>.json` → `{query, page, pending}`;
`state/failed.json` → `[{start, end, attempts}]`. Key functions:
`query_signature`, `load_checkpoint`/`save_page`, `load_pending`/`save_pending`/
`clear_pending`, `clear_window`, `load_all_windows`, `load_failed`/`save_failed`/
`add_failed`, `clear_windows`, `reset`.

### `sendlock.py`

`send_lock()` context manager + `locked` decorator — an exclusive OS file lock
on `state/rymnet_send.lock` (`fcntl.flock` on Linux, `msvcrt` fallback on
Windows). Only one process sends to Rymnet at a time; auto-released on process
exit (incl. crash).

### `signature/final_data.py`

`build_body(...)`, `send(records)` — POST a batch to Rymnet (bearer token,
`raise_for_status`). `_throttle()` enforces `RATE_LIMIT`/`RATE_WINDOW` (10/60s)
before every POST, blocking if needed.

### `signature/door_events.py`

`iter_pages(..., start_page=1)` — resumable paging (≤10 doors/chunk, 50/page),
used by the app. TLS verification disabled for the self-signed Hikvision host.

### `notifier.py`

`notify(message)` — send text to `LARK_UNION_ID`; no-op if Lark env unset, never
raises.

---

## Running under systemd

```ini
# /etc/systemd/system/hik-sync.service
[Unit]
Description=HIK -> Rymnet attendance sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hik
WorkingDirectory=/opt/fr-rymnet-integration
ExecStart=/opt/fr-rymnet-integration/.venv/bin/python main.py
Restart=always
RestartSec=10
Environment=TZ=Asia/Kuala_Lumpur

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hik-sync
journalctl -u hik-sync -f            # live logs (stdout → journald)
```

> **`WorkingDirectory` is required** — `.env`, `state/`, `logs/`, `errors/` all
> resolve relative to it.
>
> **Timezone matters** — the code hardcodes the `+08:00` offset and
> `_next_window()` uses local time. Set `TZ` (and host clock) to a matching zone
> or windows will be offset from the wall clock.
>
> **Stop the service before running manual send tools** (`--start/--end`,
> `--recover-windows`, `debug_pending.py --send`). The send lock will make them
> wait anyway, but stopping avoids contention on a long backfill.

---

## Tests

Pytest suite under `tests/`, network stubbed, temp state dir. Run with
`python -m pytest` (the `-m` form puts the repo root on `sys.path` so the
top-level modules import):

```bash
uv run python -m pytest tests/ -q
uv run python -m pytest tests/test_checkpoint.py -q     # a single file
```

Covers: page-aligned batching, send-failure → pending save, resume-after-crash,
pending-retry-first (no double-send), per-record isolation, window-retry
recovery, give-up after `MAX_WINDOW_RETRIES`, dedup logic, foreign-worker
filtering, DB status transitions, and the recover/pipeline paths.
```
