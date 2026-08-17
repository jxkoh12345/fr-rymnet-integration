import argparse
import getpass
import logging
import json
import os
import re
import time
from datetime import datetime, timedelta
from signature.door_events import iter_pages
from signature.personId import fetch_person_info
from signature.final_data import send, build_body
from signature.rymnet_employee import employee_exists, fetch_fw_roster
from DoorList import DoorList
from DeviceList import DeviceList
from ExcludeList import EXCLUDE_EMPLOYEES
import checkpoint
import db
import device_events
import sendlock
from notifier import notify

# --- Logger setup ---
os.makedirs('errors', exist_ok=True)
error_handler = logging.FileHandler('errors/errors.log', encoding='utf-8')
error_handler.setLevel(logging.ERROR)
logger = logging.getLogger(__name__)  # handlers attached in __main__ once --output-dir is known

# --- Static config ---
TIMEZONE      = '+08:00'
DOORS         = [str(k) for k, v in DoorList.items() if v['type'] == 'Door']
EVENT_TYPE    = 196893
PERSON_NAME   = ''
PERSON_ID     = ''
PERSON_CODE   = ''
TEMPERATURE_STATUS = -1
MASK_STATUS   = -1
SORT_FIELD    = 'SwipeTime'
ORDER_TYPE    = 0
BATCH_SIZE    = 100
LOG_DIR       = 'logs'  # staging sink for attendance records before send (temp until DB)
EVENT_TEST    = os.environ.get('EVENT_TEST', '')
SEND_RETRIES  = 3
SEND_RETRY_DELAY = 2   # seconds between send retries
WINDOW_MINUTES   = 30  # fetch window size
MAX_WINDOW_RETRIES = 10  # retries (one per scheduler tick) before giving up on a failed window
MIN_GAP_MINUTES  = 1   # suppress duplicate events for the same person within this window
DEVICE_SIG_KEY   = 'device'  # marks a checkpoint signature as belonging to the device path
CATCHUP_DAYS     = 3   # nightly catch-up depth: re-pull from 00:00 this many days back to now
DEVICE_POLL_SECONDS = int(os.environ.get('DEVICE_POLL_SECONDS', '120'))  # device poll cadence inside the scheduler's sleep
STEP_LOGGING     = os.environ.get('STEP_LOGGING', '').lower() == 'true'  # trace every step of the device (CJ) cycle
FOREIGN_WORKER   = os.environ.get('FOREIGN_WORKER', '').lower() == 'true'
DRY_RUN          = False  # set via --dry-run; skips Rymnet send + DB/checkpoint writes
ONLY_EMPLOYEE_NO = None   # set via --employee-no; restrict processing to this employee_no
MANUAL           = False  # True for operator-triggered runs; tags notifications so a Lark
                          # message is traceable to a person rather than the scheduler
_fw_roster_cache = None   # lazily populated by _get_fw_roster()


def _fmt(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + TIMEZONE


def reformat_time(iso_str: str) -> str:
    return datetime.fromisoformat(iso_str).strftime('%Y-%m-%d %H:%M:%S')


def _step(label: str, step: str, message: str):
    """Trace one step of the device cycle when STEP_LOGGING=true."""
    if STEP_LOGGING:
        logger.info(f"[STEP {step}] {label}: {message}")


# def notify_failure(label: str):
#     # TODO: enable notification (email / webhook / etc.)
#     pass


def _send_with_retry(records: list, label: str) -> tuple[bool, float]:
    """Send a batch, retrying up to SEND_RETRIES times.
    Returns (success, elapsed_seconds)."""
    t0 = time.perf_counter()
    times = [r['logtime'] for r in records if r.get('logtime')]
    span = f"{min(times)} → {max(times)}" if times else "—"
    for attempt in range(1, SEND_RETRIES + 1):
        try:
            result = send(records)
            elapsed = time.perf_counter() - t0
            logger.info(f"{label} [{span}] OK in {elapsed:.2f}s: {json.dumps(result)}")
            return True, elapsed
        except Exception as e:
            logger.error(f"{label} attempt {attempt}/{SEND_RETRIES} FAILED: {e}")
            if attempt < SEND_RETRIES:
                time.sleep(SEND_RETRY_DELAY)
    return False, time.perf_counter() - t0


def _log_rejected(records: list, window: str) -> str:
    """Save records Rymnet rejected individually to errors/rejected_<timestamp>.json.
    Returns the file path."""
    os.makedirs('errors', exist_ok=True)
    path = os.path.join('errors', f"rejected_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump([{'window': window, 'record': r} for r in records], f, ensure_ascii=False, indent=2)
    logger.error(f"{len(records)} record(s) rejected by Rymnet — logged to {path}")
    return path


def _log_attendance(records: list, window_start: str):
    """Stage attendance records to LOG_DIR/attendance_<date>.jsonl before sending.
    Temporary sink (one JSON object per line) until a DB replaces it."""
    if not records:
        return
    os.makedirs(LOG_DIR, exist_ok=True)
    suffix = f"_{ONLY_EMPLOYEE_NO}" if ONLY_EMPLOYEE_NO else ""
    date = datetime.fromisoformat(window_start).strftime('%Y%m%d')
    path = os.path.join(LOG_DIR, f"attendance_{date}{suffix}.jsonl")
    with open(path, 'a', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')


def _send_resilient(records: list, pages: list, signature: dict, window: str, ids: list = None) -> tuple[bool, int]:
    """Send a batch; if it fails, isolate per-record so one poison record can't
    block the rest. Returns (should_stop, num_sent).

      batch OK                     -> (False, len(records))
      fails, none send alone       -> outage: pending saved, (True, 0)
      fails, some send alone       -> log+drop the rest, advance page, (False, num_good)

    `ids` = hik_records ids aligned with records; outcomes are mirrored to
    hik_record_status (SUCCESS / PENDING / FAILED).
    """
    ids = ids if ids is not None else [None] * len(records)
    label = f"pages {pages} ({len(records)} records)"
    if DRY_RUN:
        logger.info(f"[DRY RUN] Would send {label}")
        return False, len(records)
    ok, _ = _send_with_retry(records, f"Batch {label}")
    if ok:
        db.set_status(ids, db.STATUS_SUCCESS)
        checkpoint.save_page(signature, max(pages))
        return False, len(records)

    logger.warning(f"Batch {label} failed — isolating per-record")
    good, bad, good_ids, bad_ids = [], [], [], []
    for rec, rid in zip(records, ids):
        try:
            send([rec])
            good.append(rec)
            good_ids.append(rid)
        except Exception:
            bad.append(rec)
            bad_ids.append(rid)

    if not good:
        checkpoint.save_pending(signature, pages, records, ids)
        db.set_status(ids, db.STATUS_PENDING)
        logger.error(f"Batch {label}: no records accepted individually — saved as pending, stopping.")
        notify(
            f"[HIK SYNC] Rymnet rejected whole batch — saved as pending, will auto-retry.\n"
            f"Window: {window}\n{label}\n"
            f"If it keeps failing (poison record): uv run debug_pending.py --send"
        )
        return True, 0

    if bad:
        path = _log_rejected(bad, window)
        db.set_status(bad_ids, db.STATUS_FAILED)
        notify(
            f"[HIK SYNC] {len(bad)} record(s) rejected by Rymnet — dropped from window {window}.\n"
            f"Logged to {path}\n"
            f"To fix & resend: edit the file, then uv run debug_pending.py --send {path}"
        )
    db.set_status(good_ids, db.STATUS_SUCCESS)
    checkpoint.save_page(signature, max(pages))
    return False, len(good)


def _resolve_record(item: dict, person_cache: dict) -> dict:
    pid = item['personId']
    if pid not in person_cache:
        try:
            person_cache[pid] = fetch_person_info(pid).get('personCode', '')
        except RuntimeError as e:
            logger.warning(f"{e} — skipping")
            person_cache[pid] = ''
    door_info = DoorList.get(int(item.get('doorIndexCode', 0)), {})
    return build_body(
        employee_no=person_cache[pid],
        logtime=reformat_time(item['eventTime']),
        location=door_info.get('doorName', ''),
        indicator=door_info.get('indicator') or '',
    )


def _resolve_device_record(item: dict, info: dict) -> dict:
    """One raw device event → Rymnet body. No person lookup needed: the device's
    employeeNoString is already the Artemis personCode (verified against the
    personId API), so the whole personId→personCode round trip disappears."""
    return build_body(
        employee_no=item['employeeNoString'],
        logtime=reformat_time(item['time']),
        location=info['doorName'],
        indicator=info.get('indicator') or '',
    )


def _device_id(location: str, indicator: str) -> str:
    """Strip the IN/OUT direction word from a door name to get the physical
    device (e.g. 'WHGF TURN IN 1' and 'WHGF TURN OUT 1' -> 'WHGF TURN 1'),
    since the same turnstile can register a bounce on either reader."""
    device = re.sub(rf'\b{re.escape(indicator)}\b', '', location) if indicator else location
    return ' '.join(device.split())


def _get_fw_roster() -> set:
    """Rymnet's employee_no set for category_code=FW, fetched once per process."""
    global _fw_roster_cache
    if _fw_roster_cache is None:
        _fw_roster_cache = fetch_fw_roster()
    return _fw_roster_cache


def _prepare_page(events: list, person_cache: dict, seen: set, last_sent: dict, resolve=None) -> tuple[list, list, int]:
    """Resolve a page of events into request bodies, normalize FW prefixes and
    mark duplicates. Returns (audited, deduped, dupes):
      audited — every record tagged with 'duplicate' (audit trail)
      deduped — the records to actually send
    Honors FOREIGN_WORKER by filtering both lists against Rymnet's category_code=FW roster.
    `resolve` overrides the event→body mapping (the device path passes its own);
    it defaults to the Artemis resolver."""
    resolve = resolve or _resolve_record
    bodies = [resolve(e, person_cache) for e in events]
    for record in bodies:
        if record.get('employee_no', '').startswith('FW'):
            record['employee_no'] = 'FW-' + record['employee_no'][2:]

    audited, deduped, dupes = [], [], 0
    for record in bodies:
        if EVENT_TEST and record.get('employee_no') == EVENT_TEST:
            notify(f"[HIK SYNC] Event found:\n{json.dumps(record, indent=2)}")
        emp = record.get('employee_no', '')
        logtime_str = record.get('logtime', '')
        indicator = record.get('indicator', '')
        location = record.get('location', '')
        device = _device_id(location, indicator)
        is_dup = False
        if emp:
            key = (emp, device)
            try:
                t = datetime.strptime(logtime_str, '%Y-%m-%d %H:%M:%S')
                if key in last_sent and abs((t - last_sent[key]).total_seconds()) < MIN_GAP_MINUTES * 60:
                    dupes += 1
                    is_dup = True
                    logger.debug(f"Duplicate skipped (<{MIN_GAP_MINUTES}min gap): employee_no={emp} device={device} logtime={logtime_str}")
                else:
                    last_sent[key] = t
            except ValueError:
                pass
        else:
            key = (emp, logtime_str)
            if key in seen:
                dupes += 1
                is_dup = True
                logger.debug(f"Duplicate skipped: employee_no={emp} logtime={logtime_str}")
            else:
                seen.add(key)
        audited.append({**record, 'duplicate': is_dup})
        if not is_dup:
            deduped.append(record)
    if FOREIGN_WORKER:
        fw_roster = _get_fw_roster()
        audited = [rec for rec in audited if rec.get('employee_no', '') in fw_roster]
        deduped = [rec for rec in deduped if rec.get('employee_no', '') in fw_roster]
    if ONLY_EMPLOYEE_NO:
        audited = [rec for rec in audited if rec.get('employee_no', '') == ONLY_EMPLOYEE_NO]
        deduped = [rec for rec in deduped if rec.get('employee_no', '') == ONLY_EMPLOYEE_NO]
    deduped = [rec for rec in deduped if rec.get('employee_no', '') not in EXCLUDE_EMPLOYEES]
    return audited, deduped, dupes


def run_window(start: str, end: str, reset: bool = False) -> tuple[bool, int]:
    """Fetch events in [start, end] and send to Rymnet, with checkpointing.
    Returns (ok, records_sent). ok is False if the window did not complete.
    Holds the cross-process send lock so no other process sends concurrently.
    Skipped entirely in --dry-run, since nothing is sent — dry runs can run
    alongside the live scheduler without waiting on it."""
    if DRY_RUN:
        return _run_window(start, end, reset)
    with sendlock.send_lock():
        return _run_window(start, end, reset)


def _run_window(start: str, end: str, reset: bool = False) -> tuple[bool, int]:
    cycle_start = time.perf_counter()
    logger.info(f"=== Window {start} → {end} ==={' [DRY RUN]' if DRY_RUN else ''}")

    signature = checkpoint.query_signature(start, end, DOORS, EVENT_TYPE)

    if reset and not DRY_RUN:
        checkpoint.clear_window(signature)
        logger.info("Window state cleared — starting fresh")

    # 1. Rymnet retry: re-send the batch that failed last run.
    pending = checkpoint.load_pending(signature)
    if pending:
        logger.info(f"Retrying pending batch (pages {pending['pages']}, {len(pending['records'])} records)")
        stop, _ = _send_resilient(pending['records'], pending['pages'], signature, f"{start} → {end}", pending.get('ids'))
        if stop:
            return False, 0
        if not DRY_RUN:
            checkpoint.clear_pending(signature)

    # 2. Hik resume: continue from last fully-sent page.
    resume_page = checkpoint.load_checkpoint(signature) + 1

    person_cache: dict = {}
    seen: set         = set()   # exact dedup fallback for empty employee_no
    last_sent: dict   = {}      # (employee_no, indicator) → last sent datetime
    batch: list       = []
    batch_ids: list   = []      # hik_records ids aligned with batch
    batch_pages: list = []
    total             = 0
    dupes             = 0

    def flush() -> bool:
        nonlocal batch, batch_ids, batch_pages, total
        if not batch:
            return True
        stop, sent = _send_resilient(batch, batch_pages, signature, f"{start} → {end}", batch_ids)
        if stop:
            return False
        total += sent
        batch.clear()
        batch_ids.clear()
        batch_pages.clear()
        return True

    try:
        for page_no, events in iter_pages(
            start_time=start,
            end_time=end,
            event_type=EVENT_TYPE,
            person_name=PERSON_NAME,
            person_id=PERSON_ID,
            person_code=PERSON_CODE,
            door_index_codes=DOORS,
            temperature_status=TEMPERATURE_STATUS,
            mask_status=MASK_STATUS,
            sort_field=SORT_FIELD,
            order_type=ORDER_TYPE,
            start_page=resume_page,
        ):
            audited, deduped, page_dupes = _prepare_page(events, person_cache, seen, last_sent)
            dupes += page_dupes
            _log_attendance(audited, start)
            if DRY_RUN:
                record_ids = [None] * len(audited)
            else:
                try:
                    record_ids = db.insert_records(audited)
                    failed = sum(1 for r in record_ids if r is None)
                    if failed:
                        logger.error(f"DB insert: {failed}/{len(record_ids)} record(s) failed on page {page_no}")
                    else:
                        logger.info(f"DB insert: {len(record_ids)} record(s) inserted on page {page_no}")
                except Exception as e:
                    logger.error(f"DB insert_records call failed: {e}")
                    record_ids = [None] * len(audited)
            deduped_ids = [rid for rid, rec in zip(record_ids, audited) if not rec['duplicate']]
            if batch and len(batch) + len(deduped) > BATCH_SIZE:
                if not flush():
                    return False, 0
            batch.extend(deduped)
            batch_ids.extend(deduped_ids)
            batch_pages.append(page_no)
            if len(batch) >= BATCH_SIZE:
                if not flush():
                    return False, 0
    except Exception as e:
        logger.error(f"Hik fetch error: {e} — stopping. Re-run to resume.")
        notify(
            f"[HIK SYNC] Hikvision fetch error — stopped, checkpoint saved (auto-resumes next run).\n"
            f"Window: {start} → {end}\nError: {e}"
        )
        return False, 0

    if not flush():
        return False, 0

    if not DRY_RUN:
        checkpoint.clear_window(signature)  # completed — no longer needs to rerun
    elapsed = time.perf_counter() - cycle_start
    logger.info(f"=== Window done{' [DRY RUN]' if DRY_RUN else ''} — {total} records sent, {dupes} duplicates skipped in {elapsed:.2f}s ===")
    if not DRY_RUN:
        notify(
            f"[HIK SYNC] Window done: {start} → {end}\n"
            f"{total} records sent, {dupes} duplicates skipped, {elapsed:.2f}s"
        )
    return True, total


def _device_signature(host: str) -> dict:
    """Checkpoint identity for a device. Deliberately keyed on the device alone,
    not the serial span: the cursor is the real checkpoint, and a span-based
    signature would orphan a failed cycle's pending batch as soon as the span
    moved (the next cycle would never retry it)."""
    return checkpoint.query_signature(DEVICE_SIG_KEY, host, [host], EVENT_TYPE)


def _trigger() -> str:
    """Notification suffix marking who started the run."""
    return f" (manual, {db.ACTOR})" if MANUAL else ""


def _load_last_sent(raw: dict) -> dict:
    """'<employee_no>|<device>' → datetime, for the cross-cycle dedup memory."""
    out = {}
    for key, iso in (raw or {}).items():
        emp, _, device = key.partition('|')
        try:
            out[(emp, device)] = datetime.strptime(iso, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    return out


def _dump_last_sent(last_sent: dict) -> dict:
    """Serialize the dedup memory, dropping entries too old to suppress anything.
    Aged against the newest event seen, not the wall clock, so a backfill of old
    serials still dedups correctly within itself."""
    if not last_sent:
        return {}
    cutoff = max(last_sent.values()) - timedelta(minutes=max(MIN_GAP_MINUTES * 10, 60))
    return {f"{emp}|{device}": t.strftime('%Y-%m-%d %H:%M:%S')
            for (emp, device), t in last_sent.items() if t >= cutoff}


def _log_device_gap(host: str, begin: int, end: int, expected: int, got: int) -> str:
    """Record a hole in a device's own event log — these do not heal, so the
    serial numbers are written down before the cursor moves past them."""
    os.makedirs('errors', exist_ok=True)
    path = os.path.join('errors', f"device_gap_{host.replace(':', '_')}_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'device': host, 'serial_begin': begin, 'serial_end': end,
                   'expected': expected, 'found': got, 'missing': expected - got}, f, indent=2)
    logger.error(f"{host}: {expected - got} event(s) missing from device log "
                 f"(serial {begin}-{end}) — logged to {path}")
    return path


def run_device_cycle(host: str) -> tuple[bool, int]:
    """Poll one FR terminal from its cursor to its newest serial, then send.

    Returns (ok, records_sent). The cursor only advances once everything fetched
    has been sent, so a failed cycle simply refetches the same span next time.
    """
    info = DeviceList[host]
    label = f"{host} ({info['doorName']})"
    sig = _device_signature(host)
    state = device_events.load_state(host)
    cursor = state['cursor']
    _step(label, '0/9', f"cycle start — cursor={cursor}, dedup memory={len(state['last_sent'])} entries"
                        f"{', DRY RUN' if DRY_RUN else ''}")

    # 1. Rymnet retry: re-send the batch that failed last cycle.
    pending = checkpoint.load_pending(sig)
    _step(label, '1/9', f"pending batch from last cycle: "
                        f"{len(pending['records']) if pending else 0} records")
    if pending:
        logger.info(f"{label}: retrying pending batch ({len(pending['records'])} records)")
        stop, _ = _send_resilient(pending['records'], pending['pages'], sig, label, pending.get('ids'))
        if stop:
            _step(label, '1/9', "pending batch still rejected — stopping cycle, nothing fetched")
            return False, 0
        if not DRY_RUN:
            checkpoint.clear_pending(sig)
        _step(label, '1/9', "pending batch cleared")

    newest, newest_time = device_events.newest_serial(host)
    _step(label, '2/9', f"newest serial on device = {newest} ({newest_time})")

    # 2. First run: start at the device's newest event; no backfill.
    if cursor is None:
        _step(label, '3/9', f"no cursor — bootstrapping at serial {newest}, no backfill")
        if not DRY_RUN:
            device_events.save_state(host, newest)
        logger.info(f"{label}: cursor initialised at serial {newest} ({newest_time}) — no backfill")
        if not DRY_RUN:
            notify(f"[HIK SYNC] Device {label} cursor initialised at serial {newest} ({newest_time}).\n"
                   f"Earlier events are not sent. Edit state/devices/ to backfill.")
        return True, 0

    # 3. Counter went backwards: the device's log was wiped. Never restart from 1.
    _step(label, '4/9', f"reset check: newest={newest} vs cursor={cursor} — "
                        f"{'RESET' if newest < cursor else 'nothing new' if newest == cursor else f'{newest - cursor} new serial(s)'}")
    if newest < cursor:
        logger.error(f"{label}: newest serial {newest} < cursor {cursor} — device log reset")
        notify(f"[HIK SYNC] Device {label} event log was RESET (newest serial {newest} < cursor {cursor}).\n"
               f"Cursor left untouched — fix state/devices/ manually before this device syncs again.")
        return False, 0

    if newest == cursor:
        logger.info(f"{label}: nothing new (serial {cursor})")
        return True, 0

    # 4. Fetch the whole serial span. A hole in the device's log is permanent, so
    #    salvage what arrived rather than stalling forever — but write it down.
    begin = cursor + 1
    _step(label, '5/9', f"fetching serial {begin}-{newest} (width {newest - begin + 1}), unfiltered")
    try:
        events = device_events.fetch_range(host, begin, newest)
    except device_events.DeviceGapError as e:
        _step(label, '5/9', f"gap: device holds {e.got} of {e.expected} — salvaging {len(e.events)} event(s)")
        path = _log_device_gap(host, begin, newest, e.expected, e.got)
        notify(f"[HIK SYNC] Device {label}: {e.expected - e.got} event(s) missing from its own log "
               f"(serial {begin}-{newest}).\nSending what remains. Details: {path}")
        events = e.events

    auth = device_events.auth_events(events)
    logger.info(f"{label}: serial {begin}-{newest} — {len(events)} events, {len(auth)} authentications")
    _step(label, '6/9', f"auth filter (major {device_events.AUTH_MAJOR}/minor {device_events.AUTH_MINOR}): "
                        f"{len(auth)} kept of {len(events)} fetched")

    last_sent = _load_last_sent(state['last_sent'])
    audited, deduped, dupes = _prepare_page(
        auth, {}, set(), last_sent,
        resolve=lambda item, _cache: _resolve_device_record(item, info),
    )
    _step(label, '7/9', f"resolved: {len(audited)} audited, {len(deduped)} to send, {dupes} duplicate(s) "
                        f"(<{MIN_GAP_MINUTES}min)")
    if STEP_LOGGING:
        for rec in audited:
            _step(label, '7/9', f"  {'DUP ' if rec['duplicate'] else 'send'} "
                                f"{rec.get('employee_no', '')} {rec.get('logtime', '')} "
                                f"{rec.get('indicator', '')} @ {rec.get('location', '')}")
    _log_attendance(audited, datetime.now().isoformat())

    if DRY_RUN:
        record_ids = [None] * len(audited)
    else:
        try:
            record_ids = db.insert_records(audited)
            failed = sum(1 for r in record_ids if r is None)
            if failed:
                logger.error(f"{label}: DB insert {failed}/{len(record_ids)} record(s) failed")
        except Exception as e:
            logger.error(f"{label}: DB insert_records call failed: {e}")
            record_ids = [None] * len(audited)
    deduped_ids = [rid for rid, rec in zip(record_ids, audited) if not rec['duplicate']]
    _step(label, '8/9', f"DB insert: {sum(1 for r in record_ids if r is not None)}/{len(audited)} row(s) "
                        f"got ids, {len(deduped_ids)} status row(s) to follow")

    total = 0
    for i in range(0, len(deduped), BATCH_SIZE):
        batch = deduped[i:i + BATCH_SIZE]
        _step(label, '9/9', f"sending batch {i // BATCH_SIZE + 1}/{(len(deduped) - 1) // BATCH_SIZE + 1} "
                            f"({len(batch)} records)")
        stop, sent = _send_resilient(batch, [1], sig, label, deduped_ids[i:i + BATCH_SIZE])
        if stop:
            _step(label, '9/9', f"batch rejected — cursor stays at {cursor}, span refetched next cycle")
            return False, total      # cursor stays put: this span is refetched next cycle
        total += sent

    if not DRY_RUN:
        device_events.save_state(host, newest, _dump_last_sent(last_sent))
        checkpoint.clear_window(sig)
        _step(label, '9/9', f"cursor advanced {cursor} -> {newest}, dedup memory persisted")
    logger.info(f"{label}: cycle done — {total} records sent, {dupes} duplicates skipped, cursor at {newest}")
    # Only when something was actually sent: the scheduler polls every
    # DEVICE_POLL_SECONDS, so notifying idle cycles would be pure noise.
    if not DRY_RUN and total:
        notify(
            f"[HIK SYNC] Device cycle done{_trigger()}: {label}\n"
            f"serial {begin} → {newest}\n"
            f"{total} records sent, {dupes} duplicates skipped"
        )
    return True, total


def run_device_range(host: str, start: str, end: str) -> tuple[bool, int]:
    """Replay one FR terminal over an explicit time range, then send.

    A backfill, not a poll: the cursor is never read or written, so the live
    scheduler resumes exactly where it was. Dedup memory starts empty — the
    range dedups within itself. Rymnet-side dedupe is the only guard against
    double-counting a range that was already sent.
    """
    info = DeviceList[host]
    label = f"{host} ({info['doorName']})"
    sig = _device_signature(host)
    _step(label, '0/6', f"range backfill {start} to {end}{', DRY RUN' if DRY_RUN else ''}")

    _step(label, '1/6', "fetching time range, unfiltered")
    try:
        events = device_events.fetch_time_range(host, start, end)
    except device_events.DeviceGapError as e:
        serials = [ev['serialNo'] for ev in e.events]
        _step(label, '1/6', f"gap: device holds {e.got} of {e.expected} — salvaging {len(e.events)} event(s)")
        path = _log_device_gap(host, min(serials), max(serials), e.expected, e.got)
        notify(f"[HIK SYNC] Device {label}: {e.expected - e.got} event(s) missing from its own log "
               f"({start} to {end}).\nSending what remains. Details: {path}")
        events = e.events

    auth = device_events.auth_events(events)
    logger.info(f"{label}: {start} to {end} — {len(events)} events, {len(auth)} authentications")
    _step(label, '2/6', f"auth filter (major {device_events.AUTH_MAJOR}/minor {device_events.AUTH_MINOR}): "
                        f"{len(auth)} kept of {len(events)} fetched")

    last_sent: dict = {}
    audited, deduped, dupes = _prepare_page(
        auth, {}, set(), last_sent,
        resolve=lambda item, _cache: _resolve_device_record(item, info),
    )
    _step(label, '3/6', f"resolved: {len(audited)} audited, {len(deduped)} to send, {dupes} duplicate(s) "
                        f"(<{MIN_GAP_MINUTES}min)")
    if STEP_LOGGING:
        for rec in audited:
            _step(label, '3/6', f"  {'DUP ' if rec['duplicate'] else 'send'} "
                                f"{rec.get('employee_no', '')} {rec.get('logtime', '')} "
                                f"{rec.get('indicator', '')} @ {rec.get('location', '')}")
    _log_attendance(audited, datetime.now().isoformat())

    if DRY_RUN:
        record_ids = [None] * len(audited)
    else:
        try:
            record_ids = db.insert_records(audited)
            failed = sum(1 for r in record_ids if r is None)
            if failed:
                logger.error(f"{label}: DB insert {failed}/{len(record_ids)} record(s) failed")
        except Exception as e:
            logger.error(f"{label}: DB insert_records call failed: {e}")
            record_ids = [None] * len(audited)
    deduped_ids = [rid for rid, rec in zip(record_ids, audited) if not rec['duplicate']]
    _step(label, '4/6', f"DB insert: {sum(1 for r in record_ids if r is not None)}/{len(audited)} row(s) "
                        f"got ids, {len(deduped_ids)} status row(s) to follow")

    total = 0
    for i in range(0, len(deduped), BATCH_SIZE):
        batch = deduped[i:i + BATCH_SIZE]
        _step(label, '5/6', f"sending batch {i // BATCH_SIZE + 1}/{(len(deduped) - 1) // BATCH_SIZE + 1} "
                            f"({len(batch)} records)")
        stop, sent = _send_resilient(batch, [1], sig, label, deduped_ids[i:i + BATCH_SIZE])
        if stop:
            _step(label, '5/6', "batch rejected — saved as pending, retried by the next device cycle")
            return False, total
        total += sent

    _step(label, '6/6', "range complete — cursor untouched")
    logger.info(f"{label}: range done — {total} records sent, {dupes} duplicates skipped")
    if not DRY_RUN:
        notify(
            f"[HIK SYNC] Device range done{_trigger()}: {label}\n"
            f"{start} → {end}\n"
            f"{total} records sent, {dupes} duplicates skipped (cursor untouched)"
        )
    return True, total


def run_devices_range(start: str, end: str, hosts: list = None) -> int:
    """One time-range backfill per device, under the cross-process send lock."""
    targets = hosts or list(DeviceList)
    if not targets:
        return 0

    def _range_all() -> int:
        sent = 0
        _step('devices', 'lock', f"send lock {'skipped (dry run)' if DRY_RUN else 'held'} — "
                                 f"backfilling {len(targets)} device(s) over {start} to {end}")
        for host in targets:
            try:
                ok, n = run_device_range(host, start, end)
                sent += n
                if not ok:
                    logger.warning(f"{host}: range backfill did not complete")
            except device_events.DeviceFetchError as e:
                logger.error(f"{host}: fetch failed — {e}")
            except Exception as e:
                logger.error(f"{host}: range error — {e}")
        return sent

    if DRY_RUN:
        return _range_all()
    with sendlock.send_lock():
        return _range_all()


def run_devices() -> int:
    """One poll cycle for every device in DeviceList. Holds the cross-process send
    lock so device and Artemis senders never overlap."""
    if not DeviceList:
        return 0

    def _cycle_all() -> int:
        sent = 0
        _step('devices', 'lock', f"send lock {'skipped (dry run)' if DRY_RUN else 'held'} — "
                                 f"sweeping {len(DeviceList)} device(s)")
        for host in DeviceList:
            try:
                ok, n = run_device_cycle(host)
                sent += n
                if not ok:
                    logger.warning(f"{host}: cycle did not complete — will resume next poll")
            except device_events.DeviceFetchError as e:
                logger.error(f"{host}: fetch failed — {e}")
            except Exception as e:
                logger.error(f"{host}: cycle error — {e}")
        return sent

    if DRY_RUN:
        return _cycle_all()
    with sendlock.send_lock():
        return _cycle_all()


def _next_window() -> tuple[datetime, datetime]:
    """Return (window_start, window_end) for the next 30-min boundary."""
    now = datetime.now()
    # floor to the current 30-min slot, then advance one slot
    slot = (now.minute // WINDOW_MINUTES + 1) * WINDOW_MINUTES
    boundary = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=slot)
    return boundary - timedelta(minutes=WINDOW_MINUTES), boundary


def _retry_failed_windows() -> int:
    """Retry every queued failed window once. Returns records recovered."""
    items = checkpoint.load_failed()
    if not items:
        return 0
    logger.info(f"Retrying {len(items)} failed window(s)")
    sent = 0
    remaining: list = []
    gave_up: list = []
    for it in items:
        attempt = it.get('attempts', 0) + 1
        logger.info(f"Retry window {it['start']} → {it['end']} (attempt {attempt})")
        ok, n = run_window(it['start'], it['end'])
        if ok:
            sent += n
            logger.info(f"Recovered window {it['start']} → {it['end']}")
        elif attempt >= MAX_WINDOW_RETRIES:
            gave_up.append(it)
        else:
            it['attempts'] = attempt
            remaining.append(it)
    checkpoint.save_failed(remaining)
    if gave_up:
        # mark the stuck pending records FAILED in the DB
        for it in gave_up:
            sig = checkpoint.query_signature(it['start'], it['end'], DOORS, EVENT_TYPE)
            stuck = checkpoint.load_pending(sig)
            if stuck and stuck.get('ids'):
                db.set_status(stuck['ids'], db.STATUS_FAILED)
        lines = "\n".join(f"{it['start']} → {it['end']}" for it in gave_up)
        logger.error(f"Gave up on {len(gave_up)} window(s) after {MAX_WINDOW_RETRIES} retries")
        notify(
            f"[HIK SYNC] Gave up on {len(gave_up)} window(s) after {MAX_WINDOW_RETRIES} retries — manual intervention needed:\n{lines}\n"
            f"Retry (transient outage): uv run main.py --recover-windows\n"
            f"Isolate poison records:   uv run debug_pending.py --send"
        )
    return sent


def recover_windows() -> int:
    """Re-run orphan windows (a file in state/windows/ but NOT queued in failed.json),
    resuming from each checkpoint. run_window clears the file on success; failures stay.
    Returns records recovered."""
    queued = {(it['start'], it['end']) for it in checkpoint.load_failed()}
    orphans = [q for q in checkpoint.load_all_windows()
               if (q['start'], q['end']) not in queued
               and q['start'] != DEVICE_SIG_KEY]   # device state isn't a time window; run_devices retries it
    if not orphans:
        logger.info("No orphan windows to recover")
        return 0
    logger.info(f"Recovering {len(orphans)} orphan window(s)")
    sent = 0
    for q in orphans:
        ok, n = run_window(q['start'], q['end'])
        if ok:
            sent += n
            logger.info(f"Recovered window {q['start']} → {q['end']}")
        else:
            logger.error(f"Still failing {q['start']} → {q['end']} — left in place")
    logger.info(f"Recovery done — {sent} records sent")
    return sent


def _sleep_polling_devices(until: datetime) -> int:
    """Wait for the next window boundary, polling the FR terminals every
    DEVICE_POLL_SECONDS on the way. Device fetching is cursor-based, so its
    cadence is independent of the 30-minute windows — polling often just means
    smaller spans and fresher attendance, never a missed event."""
    sent = 0
    while True:
        remaining = (until - datetime.now()).total_seconds()
        if remaining <= 0:
            return sent
        if not DeviceList:
            time.sleep(remaining)
            return sent
        time.sleep(min(DEVICE_POLL_SECONDS, remaining))
        if (until - datetime.now()).total_seconds() > 0:
            sent += run_devices()


def run_catchup() -> int:
    """Re-pull the last CATCHUP_DAYS days on both paths and send everything.

    Runs once a night, after the midnight summary. It exists to sweep up what the
    live paths can lose: an Artemis window that closed before a device uploaded,
    or a device span whose send failed. Nothing is compared against what was
    already delivered — Rymnet dedupes, so re-sending is cheaper than proving
    which records are new. The hik_records count is logged for visibility only.
    """
    now = datetime.now()
    start = (now - timedelta(days=CATCHUP_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    s, e = _fmt(start), _fmt(now)
    existing = db.count_records(start.strftime('%Y-%m-%d %H:%M:%S'), now.strftime('%Y-%m-%d %H:%M:%S'))
    held = f"{existing} record(s)" if existing >= 0 else "unknown (no DB sink)"
    logger.info(f"=== Catch-up {s} → {e} — hik_records already holds {held} ==="
                f"{' [DRY RUN]' if DRY_RUN else ''}")

    sent = 0
    if DeviceList:
        sent += run_devices_range(start=s, end=e)
    ok, n = run_window(s, e)
    sent += n
    if not ok:
        logger.error(f"Catch-up window {s} → {e} did not complete — pending batch left for retry")
    logger.info(f"Catch-up done — {sent} records sent")
    if not DRY_RUN:
        notify(f"[HIK SYNC] Catch-up done{_trigger()}: {s} → {e}\n"
               f"{sent} records sent\nhik_records held {held} for this range before the run")
    return sent


def _queue_missed_windows(after: datetime):
    """Queue every 30-min window that closed while a long job was running.
    _next_window() is forward-only, so without this a catch-up lasting past the
    next boundary would make the scheduler skip that window outright."""
    boundary = after
    while boundary + timedelta(minutes=WINDOW_MINUTES) <= datetime.now():
        win_end = boundary + timedelta(minutes=WINDOW_MINUTES)
        checkpoint.add_failed(_fmt(boundary), _fmt(win_end))
        logger.warning(f"Window {_fmt(boundary)} → {_fmt(win_end)} closed during the catch-up "
                       f"— queued for retry")
        boundary = win_end


def scheduler(reset: bool = False):
    logger.info("Scheduler started — running every 30 minutes, continuous.")
    if DeviceList:
        logger.info(f"Device path active for {', '.join(DeviceList)} — polling every {DEVICE_POLL_SECONDS}s")
    if reset:
        checkpoint.reset()
        logger.info("State reset — starting fresh")
    daily_total = 0
    while True:
        win_start, win_end = _next_window()
        sleep_secs = (win_end - datetime.now()).total_seconds()
        logger.info(f"Sleeping {sleep_secs:.0f}s until window {_fmt(win_start)} → {_fmt(win_end)}")
        daily_total += _sleep_polling_devices(win_end)

        daily_total += run_devices()      # catch events right up to the boundary
        daily_total += _retry_failed_windows()

        ok, n = run_window(_fmt(win_start), _fmt(win_end))
        if ok:
            daily_total += n
        else:
            checkpoint.add_failed(_fmt(win_start), _fmt(win_end))

        # Last window of the day: 23:30→00:00, processed at midnight
        if win_end.hour == 0 and win_end.minute == 0:
            date_label = win_start.strftime('%Y-%m-%d')
            notify(f"[HIK SYNC] Daily summary {date_label}: {daily_total} records sent")
            logger.info(f"Daily summary {date_label}: {daily_total} records sent")
            daily_total = 0
            run_catchup()
            _queue_missed_windows(win_end)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Hik → Rymnet attendance sync (continuous, resumable)")
    parser.add_argument('--reset', action='store_true',
                        help="Clear saved checkpoint/pending before starting")
    parser.add_argument('--clear-windows', action='store_true',
                        help="Delete all files in state/windows/ and exit")
    parser.add_argument('--recover-windows', action='store_true',
                        help="Re-run orphan windows in state/windows/ (resume from checkpoint), then exit")
    parser.add_argument('--devices-once', action='store_true',
                        help="Run one device poll cycle per DeviceList entry (cursor-based), then exit")
    parser.add_argument('--catchup', action='store_true',
                        help=f"Run the nightly catch-up now: re-pull both paths from 00:00 "
                             f"{CATCHUP_DAYS} days ago to now and send, then exit")
    parser.add_argument('--devices-start', metavar='DATETIME',
                        help="Device backfill: range start, e.g. 2026-08-10T00:00:00 (cursor untouched)")
    parser.add_argument('--devices-end', metavar='DATETIME',
                        help="Device backfill: range end,   e.g. 2026-08-11T00:00:00")
    parser.add_argument('--device-host', metavar='IP', action='append',
                        help="Limit --devices-start/--devices-end to this device (repeatable; default: all)")
    parser.add_argument('--start', metavar='DATETIME',
                        help="Test mode: window start, e.g. 2026-04-01T08:00:00")
    parser.add_argument('--end', metavar='DATETIME',
                        help="Test mode: window end,   e.g. 2026-04-01T08:30:00")
    parser.add_argument('--output-dir', metavar='DIR', default='logs',
                        help="Directory for attendance.log (default: logs)")
    parser.add_argument('--dry-run', action='store_true',
                        help="Fetch/process events but don't send to Rymnet or write DB/checkpoint state")
    parser.add_argument('--employee-no', metavar='EMPLOYEE_NO',
                        help="Only log/send records for this employee_no (verified against Rymnet first)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    attendance_handler = logging.FileHandler(os.path.join(args.output_dir, 'attendance.log'), encoding='utf-8')
    attendance_handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[attendance_handler, error_handler],
    )
    DRY_RUN = args.dry_run

    if args.employee_no:
        if not employee_exists(args.employee_no):
            logger.error(f"employee_no {args.employee_no} not found in Rymnet — aborting")
            raise SystemExit(1)
        logger.info(f"employee_no {args.employee_no} confirmed — restricting to this employee")
        ONLY_EMPLOYEE_NO = args.employee_no

    if args.clear_windows:
        checkpoint.clear_windows()
        logger.info("Cleared state/windows/")
        raise SystemExit(0)

    if args.recover_windows:
        db.ACTOR = getpass.getuser()   # manual rerun — audit as the user
        recover_windows()
        raise SystemExit(0)

    if args.catchup:
        db.ACTOR = getpass.getuser()   # manual run — audit as the user
        MANUAL = True
        run_catchup()
        raise SystemExit(0)

    if args.devices_once:
        db.ACTOR = getpass.getuser()   # manual run — audit as the user
        MANUAL = True
        sent = run_devices()
        logger.info(f"Device poll done — {sent} records sent")
        if not DRY_RUN:
            notify(f"[HIK SYNC] Manual device poll finished ({db.ACTOR}): "
                   f"{sent} records sent across {len(DeviceList)} device(s)")
        raise SystemExit(0)

    if args.devices_start or args.devices_end:
        if not (args.devices_start and args.devices_end):
            parser.error("--devices-start and --devices-end must be used together")
        hosts = args.device_host or list(DeviceList)
        unknown = [h for h in hosts if h not in DeviceList]
        if unknown:
            parser.error(f"--device-host not in DeviceList: {', '.join(unknown)}")
        db.ACTOR = getpass.getuser()   # manual rerun — audit as the user
        MANUAL = True
        start = args.devices_start if '+' in args.devices_start else args.devices_start + TIMEZONE
        end   = args.devices_end   if '+' in args.devices_end   else args.devices_end   + TIMEZONE
        if not DRY_RUN:
            notify(f"[HIK SYNC] Manual device backfill started ({db.ACTOR}): {start} → {end}\n"
                   f"{len(hosts)} device(s): {', '.join(hosts)}")
        sent = run_devices_range(start=start, end=end, hosts=hosts)
        logger.info(f"Device range backfill done — {sent} records sent")
        if not DRY_RUN:
            notify(f"[HIK SYNC] Manual device backfill finished ({db.ACTOR}): "
                   f"{sent} records sent, {start} → {end}")
        raise SystemExit(0)

    if args.start or args.end:
        if not (args.start and args.end):
            parser.error("--start and --end must be used together")
        db.ACTOR = getpass.getuser()   # manual rerun — audit as the user
        run_window(
            start=args.start if '+' in args.start else args.start + TIMEZONE,
            end=args.end   if '+' in args.end   else args.end   + TIMEZONE,
            reset=args.reset,
        )
    else:
        scheduler(reset=args.reset)
