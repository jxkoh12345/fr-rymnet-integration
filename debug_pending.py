"""Isolate which pending record(s) Rymnet rejects, then unstick the window.

Rymnet's /attendance/set is all-or-nothing: one bad record 500s the whole batch
with a generic message, so you can't tell which of the ~88 is at fault. This
loads the stuck pending batch and either:

  * inspects it for obvious problems (default, no API calls), or
  * sends each record alone (--send) to find exactly which Rymnet rejects.

With --send, the good records are delivered (Rymnet dedupes, so re-sends are
harmless), the rejected ones are logged to errors/ and DROPPED, and the window's
pending batch is cleared + its checkpoint advanced so main.py resumes past it.

Usage:
  uv run debug_pending.py              # inspect: flag suspicious records, no sending
  uv run debug_pending.py --send       # isolate, drop+log bad records, unstick windows
  uv run debug_pending.py --send FILE  # ad-hoc: send records from a JSON file (a list)
"""
import argparse
import glob
import json
import os
import re
from datetime import datetime

from signature.final_data import send

_WINDOWS = os.path.join('state', 'windows', '*.json')
_TIME_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')


def _pending_windows():
    """Yield (path, pages, records) for each window file holding a pending batch."""
    for path in sorted(glob.glob(_WINDOWS)):
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        pending = data.get('pending')
        if pending and pending.get('records'):
            yield path, pending.get('pages') or [], pending['records']


def _issues(rec: dict) -> list:
    out = []
    if not rec.get('employee_no'):
        out.append('empty employee_no')
    if not _TIME_RE.match(rec.get('logtime', '')):
        out.append(f"bad logtime {rec.get('logtime')!r}")
    if not rec.get('location'):
        out.append('empty location')
    return out


def inspect(records: list):
    flagged = 0
    for i, rec in enumerate(records, 1):
        issues = _issues(rec)
        if issues:
            flagged += 1
            print(f"  [{i}] {rec.get('employee_no')!r} {rec.get('logtime')} - {', '.join(issues)}")
    print(f"  {flagged}/{len(records)} records look suspicious.")
    if not flagged:
        print("  No obvious structural problems - run with --send to isolate by sending.")


def send_each(records: list) -> list:
    """Send every record on its own; return the ones Rymnet rejects."""
    failed = []
    for i, rec in enumerate(records, 1):
        try:
            send([rec])
            print(f"  [{i}/{len(records)}] OK   {rec.get('employee_no')} {rec.get('logtime')}")
        except Exception as e:
            print(f"  [{i}/{len(records)}] FAIL {rec.get('employee_no')} {rec.get('logtime')}\n        {e}")
            failed.append(rec)
    return failed


def _log_bad(failed: list) -> str:
    os.makedirs('errors', exist_ok=True)
    out = os.path.join('errors', f"bad_records_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(failed, f, indent=2, ensure_ascii=False)
    return out


def _unstick(path: str, pages: list):
    """Clear this window's pending batch and advance its page, so main.py resumes."""
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    data['page'] = max(data.get('page', 0), max(pages) if pages else 0)
    data['pending'] = None
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f)


def main():
    parser = argparse.ArgumentParser(description="Isolate which pending record(s) Rymnet rejects")
    parser.add_argument('--send', action='store_true',
                        help="Send each record alone to find failures, then drop+log bad ones and unstick the window")
    parser.add_argument('file', nargs='?',
                        help="JSON file with a list of records; default scans state/windows/")
    args = parser.parse_args()

    # ad-hoc file mode: no state to unstick, just inspect or send
    if args.file:
        with open(args.file, encoding='utf-8') as f:
            records = json.load(f)
        if not records:
            print("File has no records.")
            return
        print(f"Loaded {len(records)} records from {args.file}.")
        if args.send:
            failed = send_each(records)
            print(f"\n=== {len(failed)}/{len(records)} rejected by Rymnet ===")
            if failed:
                print(f"Rejected records -> {_log_bad(failed)}")
        else:
            inspect(records)
        return

    # state mode: process each stuck window file
    windows = list(_pending_windows())
    if not windows:
        print("No pending batches in state/windows/.")
        return

    for path, pages, records in windows:
        print(f"\n=== {path}: {len(records)} pending records (pages {pages}) ===")
        if not args.send:
            inspect(records)
            continue
        failed = send_each(records)
        print(f"  {len(failed)}/{len(records)} rejected by Rymnet")
        if failed:
            print(f"  Rejected records -> {_log_bad(failed)}")
        _unstick(path, pages)
        print(f"  Pending cleared, checkpoint advanced to page {max(pages) if pages else 0} - main.py will resume past it.")


if __name__ == '__main__':
    main()
