"""Best-effort Postgres sink for attendance records + send status.

  hik_records        — every fetched record, duplicates included (audit trail).
  hik_record_status  — one row per record that was actually ready to send:
                       SUCCESS | PENDING | FAILED. Duplicates get no row.

All functions no-op when PG_* env vars are unset, and DB errors are logged
but never raised — the sync must keep working without the DB.
"""
import logging
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

STATUS_SUCCESS = 'SUCCESS'
STATUS_PENDING = 'PENDING'
STATUS_FAILED  = 'FAILED'

# Who is acting: 'system' for the scheduler; main.py/debug_pending.py set the
# OS username here for manual reruns (recorded in modified_by on updates).
ACTOR = 'system'

_CONN = {
    'host':     os.environ.get('PG_HOST', ''),
    'port':     os.environ.get('PG_PORT', ''),
    'dbname':   os.environ.get('PG_DATABASE', ''),
    'user':     os.environ.get('PG_USER', ''),
    'password': os.environ.get('PG_PASSWORD', ''),
}


def enabled() -> bool:
    return bool(_CONN['host'] and _CONN['dbname'])


def _connect():
    return psycopg.connect(**{k: v for k, v in _CONN.items() if v})


def insert_records(records: list) -> list:
    """Insert every record (duplicates included) into hik_records.
    Returns the new ids, aligned with `records` ([None]*n when disabled/failed)."""
    if not enabled() or not records:
        return [None] * len(records)
    try:
        with _connect() as conn, conn.cursor() as cur:
            ids = []
            for r in records:
                cur.execute(
                    "INSERT INTO hik_records"
                    " (employee_no, logtime, location, indicator, remark, date_created, created_by)"
                    " VALUES (%s, %s, %s, %s, %s, now(), 'system') RETURNING id",
                    (r.get('employee_no'), r.get('logtime'), r.get('location'),
                     r.get('indicator'), r.get('remarks')),
                )
                ids.append(cur.fetchone()[0])
            return ids
    except Exception as e:
        logger.error(f"DB insert_records failed: {e}")
        return [None] * len(records)


def set_status(record_ids: list, status: str):
    """Upsert hik_record_status for the given hik_records ids.
    First write for an id INSERTs (created_by='system'); later transitions
    UPDATE it (modified_by=ACTOR). None ids (DB was down on insert) are skipped."""
    ids = [i for i in (record_ids or []) if i is not None]
    if not enabled() or not ids:
        return
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE hik_record_status SET status=%s, date_modified=now(), modified_by=%s"
                " WHERE hik_record_id = ANY(%s) AND date_deleted IS NULL"
                " RETURNING hik_record_id",
                (status, ACTOR, ids),
            )
            updated = {row[0] for row in cur.fetchall()}
            for rid in ids:
                if rid not in updated:
                    cur.execute(
                        "INSERT INTO hik_record_status"
                        " (hik_record_id, status, date_created, created_by)"
                        " VALUES (%s, %s, now(), 'system')",
                        (rid, status),
                    )
    except Exception as e:
        logger.error(f"DB set_status({status}) failed: {e}")
