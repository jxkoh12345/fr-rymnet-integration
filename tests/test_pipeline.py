"""
Test script — simulates the producer/consumer pipeline with fake data.
Delays mimic real Hik page fetch and rymnet send latency.
"""
import logging
import json
import threading
import queue
import time
from datetime import datetime

from signature.final_data import build_body
from DoorList import DoorList

# --- Logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
_SENTINEL = object()

# Delays (seconds) — tweak to see different concurrency behaviour
HIK_PAGE_DELAY  = 2.0   # simulates Hik API response time per page
SEND_DELAY      = 0.1   # simulates rymnet API response time per batch
PERSON_DELAY    = 0.5   # simulates personId API response time (first lookup only)

_DOOR_KEYS = [k for k, v in DoorList.items() if v['type'] == 'Door']


def _fake_events(total: int):
    PAGE_SIZE = 50
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    count = 0
    for page in range(1, pages + 1):
        logger.info(f"[HIK] Fetching page {page}/{pages}...")
        time.sleep(HIK_PAGE_DELAY)
        for i in range(PAGE_SIZE):
            if count >= total:
                return
            door_key = _DOOR_KEYS[count % len(_DOOR_KEYS)]
            yield {
                'eventId':       f'EVT{count:05d}',
                'personId':      f'P{(count % 10) + 1:03d}',
                'eventTime':     '2026-04-08T08:26:24+08:00',
                'doorIndexCode': str(door_key),
            }
            count += 1
        logger.info(f"[HIK] Page {page}/{pages} done — {count}/{total} events yielded")


def _fake_fetch_person(person_id: str) -> dict:
    time.sleep(PERSON_DELAY)
    return {'personCode': f'EMP{person_id}', 'personName': f'Person {person_id}'}


def reformat_time(iso_str: str) -> str:
    return datetime.fromisoformat(iso_str).strftime('%Y-%m-%d %H:%M:%S')


def _send_batch(batch: list, batch_num: int):
    logger.info(f"[SEND] ── Batch {batch_num} start ({len(batch)} records) ──")
    for record in batch:
        logger.info(f"  {json.dumps(record)}")
    logger.info(f"[SEND] Waiting for rymnet response (simulated {SEND_DELAY}s)...")
    time.sleep(SEND_DELAY)
    logger.info(f"[SEND] ── Batch {batch_num} done ──")


def main():
    TOTAL_FAKE_EVENTS = 350   # 3 full batches of 100 + 1 partial batch of 50

    event_queue: queue.Queue = queue.Queue(maxsize=500)

    def producer():
        logger.info("[PRODUCER] Starting...")
        try:
            count = 0
            for event in _fake_events(TOTAL_FAKE_EVENTS):
                event_queue.put(event)
                count += 1
            logger.info(f"[PRODUCER] Done — {count} events queued")
        except Exception as e:
            logger.error(f"[PRODUCER] Error: {e}")
        finally:
            event_queue.put(_SENTINEL)

    threading.Thread(target=producer, daemon=True).start()

    person_cache = {}
    batch = []
    batch_num = 0
    total = 0

    logger.info("[CONSUMER] Starting...")

    while True:
        item = event_queue.get()
        if item is _SENTINEL:
            logger.info("[CONSUMER] Received sentinel — producer finished")
            break

        pid = item['personId']
        if pid not in person_cache:
            logger.info(f"[CONSUMER] Resolving personId {pid}...")
            person_cache[pid] = _fake_fetch_person(pid).get('personCode', '')
            logger.info(f"[CONSUMER] {pid} → {person_cache[pid]}")

        door_info = DoorList.get(int(item.get('doorIndexCode', 0)), {})
        batch.append(build_body(
            employee_no=person_cache[pid],
            logtime=reformat_time(item['eventTime']),
            location=door_info.get('doorName', ''),
            indicator=door_info.get('indicator') or '',
        ))
        total += 1
        logger.info(f"[CONSUMER] Event {item['eventId']} added — batch {batch_num + 1}: {len(batch)}/{BATCH_SIZE}")

        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            _send_batch(batch, batch_num)
            batch = []

    if batch:
        batch_num += 1
        _send_batch(batch, batch_num)

    logger.info(f"[DONE] {total} events processed, {batch_num} batch(es) sent")


if __name__ == '__main__':
    main()
