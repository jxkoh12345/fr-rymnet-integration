"""Backfill hik_records from Hikvision for a window — no Rymnet send, no state.

Inserts every fetched record, duplicates included (a duplicate is visible by
having no hik_record_status row). hik_record_status is left untouched: it gets
filled when the scheduler (or a manual main.py rerun) actually sends — that
run inserts its own fresh hik_records rows, so reruns always add new entries.

Usage:
  uv run fill_records.py --start 2026-07-01T08:00:00 --end 2026-07-01T08:30:00
"""
import argparse

import db
import main


def fill(start: str, end: str) -> tuple[int, int]:
    """Fetch every page in [start, end] and insert into hik_records.
    Returns (total_records, duplicates)."""
    person_cache: dict = {}
    seen: set = set()
    last_sent: dict = {}
    total = dupes = 0
    for page_no, events in main.iter_pages(
        start_time=start,
        end_time=end,
        event_type=main.EVENT_TYPE,
        person_name=main.PERSON_NAME,
        person_id=main.PERSON_ID,
        person_code=main.PERSON_CODE,
        door_index_codes=main.DOORS,
        temperature_status=main.TEMPERATURE_STATUS,
        mask_status=main.MASK_STATUS,
        sort_field=main.SORT_FIELD,
        order_type=main.ORDER_TYPE,
        start_page=1,
    ):
        audited, _, page_dupes = main._prepare_page(events, person_cache, seen, last_sent)
        ids = db.insert_records(audited)
        inserted = sum(1 for i in ids if i is not None)
        total += len(audited)
        dupes += page_dupes
        print(f"Page {page_no}: {len(audited)} records ({page_dupes} dups), {inserted} inserted")
    return total, dupes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Backfill hik_records from Hikvision (no Rymnet send)")
    parser.add_argument('--start', required=True, metavar='DATETIME', help="e.g. 2026-07-01T08:00:00")
    parser.add_argument('--end', required=True, metavar='DATETIME', help="e.g. 2026-07-01T08:30:00")
    args = parser.parse_args()

    if not db.enabled():
        raise SystemExit("PG_* credentials not set in .env — nothing to insert into.")

    start = args.start if '+' in args.start else args.start + main.TIMEZONE
    end = args.end if '+' in args.end else args.end + main.TIMEZONE
    total, dupes = fill(start, end)
    print(f"Done — {total} record(s) staged to hik_records ({dupes} duplicate(s) included).")
