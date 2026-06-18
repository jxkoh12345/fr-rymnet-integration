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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()],
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
        label = f"Pending batch (pages {pending['pages']}, {len(pending['records'])} records)"
        logger.info(f"Retrying {label}")
        ok, elapsed = _send_with_retry(pending['records'], label)
        if ok:
            checkpoint.save_page(signature, max(pending['pages']))
            checkpoint.clear_pending(signature)
        else:
            logger.error("Pending batch still failing — stopping.")
            notify(f"[HIK SYNC] Pending batch still failing.\nWindow: {start} → {end}")
            return False, 0

    # 2. Hik resume: continue from last fully-sent page.
    resume_page = checkpoint.load_checkpoint(signature) + 1

    person_cache: dict = {}
    batch: list       = []
    batch_pages: list = []
    total             = 0

    def flush() -> bool:
        nonlocal batch, batch_pages, total
        if not batch:
            return True
        label = f"Batch pages {batch_pages} ({len(batch)} records)"
        ok, elapsed = _send_with_retry(batch, label)
        if ok:
            checkpoint.save_page(signature, max(batch_pages))
            total += len(batch)
            batch.clear()
            batch_pages.clear()
            return True
        checkpoint.save_pending(signature, batch_pages, batch)
        logger.error(f"{label} failed after {SEND_RETRIES} retries — saved as pending, stopping.")
        notify(f"[HIK SYNC] Rymnet send failed after {SEND_RETRIES} retries — batch saved, will retry.\nWindow: {start} → {end}\n{label}")
        return False

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
            for record in bodies:
                if EVENT_TEST and record.get('employee_no') == EVENT_TEST:
                    notify(f"[HIK SYNC] Event found:\n{json.dumps(record, indent=2)}")
            if batch and len(batch) + len(bodies) > BATCH_SIZE:
                if not flush():
                    return False, 0
            batch.extend(bodies)
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
    logger.info(f"=== Window done — {total} records sent in {elapsed:.2f}s ===")
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
    parser.add_argument('--start', metavar='DATETIME',
                        help="Test mode: window start, e.g. 2026-04-01T08:00:00")
    parser.add_argument('--end', metavar='DATETIME',
                        help="Test mode: window end,   e.g. 2026-04-01T08:30:00")
    args = parser.parse_args()

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
