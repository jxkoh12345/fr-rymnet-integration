import argparse
import logging
import json
import os
import time
from datetime import datetime, timedelta
from signature.door_events import iter_pages
from signature.personId import fetch_person_info
from signature.final_data import send, build_body
from DoorList import DoorList
import checkpoint
from notifier import notify

# --- Logger setup ---
os.makedirs('errors', exist_ok=True)
error_handler = logging.FileHandler('errors/errors.log', encoding='utf-8')
error_handler.setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(), error_handler],
)
logger = logging.getLogger(__name__)

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
ORDER_TYPE    = 1
BATCH_SIZE    = 100
EVENT_TEST    = os.environ.get('EVENT_TEST', '')
SEND_RETRIES  = 3
SEND_RETRY_DELAY = 2   # seconds between send retries
WINDOW_MINUTES   = 30  # fetch window size
MAX_WINDOW_RETRIES = 10  # retries (one per scheduler tick) before giving up on a failed window
MIN_GAP_MINUTES  = 5   # suppress duplicate events for the same person within this window
FOREIGN_WORKER   = os.environ.get('FOREIGN_WORKER', '').lower() == 'true'


def _fmt(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + TIMEZONE


def reformat_time(iso_str: str) -> str:
    return datetime.fromisoformat(iso_str).strftime('%Y-%m-%d %H:%M:%S')


# def notify_failure(label: str):
#     # TODO: enable notification (email / webhook / etc.)
#     pass


def _send_with_retry(records: list, label: str) -> tuple[bool, float]:
    """Send a batch, retrying up to SEND_RETRIES times.
    Returns (success, elapsed_seconds)."""
    t0 = time.perf_counter()
    for attempt in range(1, SEND_RETRIES + 1):
        try:
            result = send(records)
            elapsed = time.perf_counter() - t0
            logger.info(f"{label} OK in {elapsed:.2f}s: {json.dumps(result)}")
            return True, elapsed
        except Exception as e:
            logger.error(f"{label} attempt {attempt}/{SEND_RETRIES} FAILED: {e}")
            if attempt < SEND_RETRIES:
                time.sleep(SEND_RETRY_DELAY)
    return False, time.perf_counter() - t0


def _log_rejected(records: list, window: str):
    """Append records Rymnet rejected individually to errors/rejected.jsonl."""
    os.makedirs('errors', exist_ok=True)
    with open('errors/rejected.jsonl', 'a', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps({'window': window, 'record': r}, ensure_ascii=False) + '\n')
    logger.error(f"{len(records)} record(s) rejected by Rymnet — logged to errors/rejected.jsonl")


def _send_resilient(records: list, pages: list, signature: dict, window: str) -> tuple[bool, int]:
    """Send a batch; if it fails, isolate per-record so one poison record can't
    block the rest. Returns (should_stop, num_sent).

      batch OK                     -> (False, len(records))
      fails, none send alone       -> outage: pending saved, (True, 0)
      fails, some send alone       -> log+drop the rest, advance page, (False, num_good)
    """
    label = f"pages {pages} ({len(records)} records)"
    ok, _ = _send_with_retry(records, f"Batch {label}")
    if ok:
        checkpoint.save_page(signature, max(pages))
        return False, len(records)

    logger.warning(f"Batch {label} failed — isolating per-record")
    good, bad = [], []
    for rec in records:
        try:
            send([rec])
            good.append(rec)
        except Exception:
            bad.append(rec)

    if not good:
        checkpoint.save_pending(signature, pages, records)
        logger.error(f"Batch {label}: no records accepted individually — saved as pending, stopping.")
        notify(f"[HIK SYNC] Rymnet rejecting whole batch — saved pending, will retry.\nWindow: {window}\n{label}")
        return True, 0

    if bad:
        _log_rejected(bad, window)
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


def run_window(start: str, end: str, reset: bool = False) -> tuple[bool, int]:
    """Fetch events in [start, end] and send to Rymnet, with checkpointing.
    Returns (ok, records_sent). ok is False if the window did not complete."""
    cycle_start = time.perf_counter()
    logger.info(f"=== Window {start} → {end} ===")

    signature = checkpoint.query_signature(start, end, DOORS, EVENT_TYPE)

    if reset:
        checkpoint.clear_window(signature)
        logger.info("Window state cleared — starting fresh")

    # 1. Rymnet retry: re-send the batch that failed last run.
    pending = checkpoint.load_pending(signature)
    if pending:
        logger.info(f"Retrying pending batch (pages {pending['pages']}, {len(pending['records'])} records)")
        stop, _ = _send_resilient(pending['records'], pending['pages'], signature, f"{start} → {end}")
        if stop:
            return False, 0
        checkpoint.clear_pending(signature)

    # 2. Hik resume: continue from last fully-sent page.
    resume_page = checkpoint.load_checkpoint(signature) + 1

    person_cache: dict = {}
    seen: set         = set()   # exact dedup fallback for empty employee_no
    last_sent: dict   = {}      # employee_no → last sent datetime
    batch: list       = []
    batch_pages: list = []
    total             = 0
    dupes             = 0

    def flush() -> bool:
        nonlocal batch, batch_pages, total
        if not batch:
            return True
        stop, sent = _send_resilient(batch, batch_pages, signature, f"{start} → {end}")
        if stop:
            return False
        total += sent
        batch.clear()
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
            bodies = [_resolve_record(e, person_cache) for e in events]
            deduped = []
            for record in bodies:
                if EVENT_TEST and record.get('employee_no') == EVENT_TEST:
                    notify(f"[HIK SYNC] Event found:\n{json.dumps(record, indent=2)}")
                emp = record.get('employee_no', '')
                logtime_str = record.get('logtime', '')
                if emp:
                    try:
                        t = datetime.strptime(logtime_str, '%Y-%m-%d %H:%M:%S')
                        if emp in last_sent and abs((t - last_sent[emp]).total_seconds()) < MIN_GAP_MINUTES * 60:
                            dupes += 1
                            logger.debug(f"Duplicate skipped (<{MIN_GAP_MINUTES}min gap): employee_no={emp} logtime={logtime_str}")
                            continue
                        last_sent[emp] = t
                    except ValueError:
                        pass
                else:
                    key = (emp, logtime_str)
                    if key in seen:
                        dupes += 1
                        logger.debug(f"Duplicate skipped: employee_no={emp} logtime={logtime_str}")
                        continue
                    seen.add(key)
                deduped.append(record)
            for rec in deduped:
                if rec.get('employee_no', '').startswith('FW'):
                    rec['employee_no'] = 'FW-' + rec['employee_no'][2:]
            if FOREIGN_WORKER:
                deduped = [rec for rec in deduped if rec.get('employee_no', '').startswith('FW-')]
            if batch and len(batch) + len(deduped) > BATCH_SIZE:
                if not flush():
                    return False, 0
            batch.extend(deduped)
            batch_pages.append(page_no)
            if len(batch) >= BATCH_SIZE:
                if not flush():
                    return False, 0
    except Exception as e:
        logger.error(f"Hik fetch error: {e} — stopping. Re-run to resume.")
        notify(f"[HIK SYNC] Hikvision fetch error — stopped, checkpoint saved.\nWindow: {start} → {end}\nError: {e}")
        return False, 0

    if not flush():
        return False, 0

    checkpoint.clear_window(signature)  # completed — no longer needs to rerun
    elapsed = time.perf_counter() - cycle_start
    logger.info(f"=== Window done — {total} records sent, {dupes} duplicates skipped in {elapsed:.2f}s ===")
    return True, total


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
        lines = "\n".join(f"{it['start']} → {it['end']}" for it in gave_up)
        logger.error(f"Gave up on {len(gave_up)} window(s) after {MAX_WINDOW_RETRIES} retries")
        notify(f"[HIK SYNC] Gave up on {len(gave_up)} window(s) after {MAX_WINDOW_RETRIES} retries — manual intervention needed:\n{lines}")
    return sent


def recover_windows() -> int:
    """Re-run orphan windows (a file in state/windows/ but NOT queued in failed.json),
    resuming from each checkpoint. run_window clears the file on success; failures stay.
    Returns records recovered."""
    queued = {(it['start'], it['end']) for it in checkpoint.load_failed()}
    orphans = [q for q in checkpoint.load_all_windows()
               if (q['start'], q['end']) not in queued]
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


def scheduler(reset: bool = False):
    logger.info("Scheduler started — running every 30 minutes, continuous.")
    if reset:
        checkpoint.reset()
        logger.info("State reset — starting fresh")
    daily_total = 0
    while True:
        win_start, win_end = _next_window()
        sleep_secs = (win_end - datetime.now()).total_seconds()
        logger.info(f"Sleeping {sleep_secs:.0f}s until window {_fmt(win_start)} → {_fmt(win_end)}")
        time.sleep(max(0, sleep_secs))

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Hik → Rymnet attendance sync (continuous, resumable)")
    parser.add_argument('--reset', action='store_true',
                        help="Clear saved checkpoint/pending before starting")
    parser.add_argument('--clear-windows', action='store_true',
                        help="Delete all files in state/windows/ and exit")
    parser.add_argument('--recover-windows', action='store_true',
                        help="Re-run orphan windows in state/windows/ (resume from checkpoint), then exit")
    parser.add_argument('--start', metavar='DATETIME',
                        help="Test mode: window start, e.g. 2026-04-01T08:00:00")
    parser.add_argument('--end', metavar='DATETIME',
                        help="Test mode: window end,   e.g. 2026-04-01T08:30:00")
    args = parser.parse_args()

    if args.clear_windows:
        checkpoint.clear_windows()
        logger.info("Cleared state/windows/")
        raise SystemExit(0)

    if args.recover_windows:
        recover_windows()
        raise SystemExit(0)

    if args.start or args.end:
        if not (args.start and args.end):
            parser.error("--start and --end must be used together")
        run_window(
            start=args.start if '+' in args.start else args.start + TIMEZONE,
            end=args.end   if '+' in args.end   else args.end   + TIMEZONE,
            reset=args.reset,
        )
    else:
        scheduler(reset=args.reset)
