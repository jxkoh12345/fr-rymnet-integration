"""Backfill hik_records_test from the FR (Hikvision) system for a window — test only, no Rymnet send.

hik_records_test is a scratch clone of hik_records; drop it when done testing.

Usage:
  uv run fill_records_test.py --start 2026-07-01T08:00:00 --end 2026-07-01T08:30:00
"""
import argparse

import db
import main


def _insert_test(records: list) -> list:
    """Same as db.insert_records but targets hik_records_test."""
    if not db.enabled() or not records:
        return [None] * len(records)
    try:
        with db._connect() as conn, conn.cursor() as cur:
            ids = []
            for r in records:
                cur.execute(
                    "INSERT INTO hik_records_test"
                    " (employee_no, logtime, location, indicator, remark, date_created, created_by)"
                    " VALUES (%s, %s, %s, %s, %s, now(), 'system') RETURNING id",
                    (r.get('employee_no'), r.get('logtime'), r.get('location'),
                     r.get('indicator'), r.get('remarks')),
                )
                ids.append(cur.fetchone()[0])
            return ids
    except Exception as e:
        print(f"DB insert failed: {e}")
        return [None] * len(records)


def fill(start: str, end: str) -> tuple[int, int]:
    """Fetch every page in [start, end] and insert into hik_records_test.
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
        ids = _insert_test(audited)
        inserted = sum(1 for i in ids if i is not None)
        total += len(audited)
        dupes += page_dupes
        print(f"Page {page_no}: {len(audited)} records ({page_dupes} dups), {inserted} inserted")
    return total, dupes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Backfill hik_records_test from FR system (test only, no Rymnet send)")
    parser.add_argument('--start', required=True, metavar='DATETIME', help="e.g. 2026-07-01T08:00:00")
    parser.add_argument('--end', required=True, metavar='DATETIME', help="e.g. 2026-07-01T08:30:00")
    args = parser.parse_args()

    if not db.enabled():
        raise SystemExit("PG_* credentials not set in .env — nothing to insert into.")

    start = args.start if '+' in args.start else args.start + main.TIMEZONE
    end = args.end if '+' in args.end else args.end + main.TIMEZONE
    total, dupes = fill(start, end)
    print(f"Done — {total} record(s) staged to hik_records_test ({dupes} duplicate(s) included).")
