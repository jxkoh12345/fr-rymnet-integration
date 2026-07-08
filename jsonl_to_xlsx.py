"""Convert an attendance .jsonl file (one JSON object per line) to .xlsx.

Usage:
  uv run jsonl_to_xlsx.py logs/attendance_20260708.jsonl
  uv run jsonl_to_xlsx.py logs/attendance_20260708.jsonl -o out.xlsx
"""
import argparse
import json
import os

from openpyxl import Workbook


def convert(in_path: str, out_path: str):
    rows = []
    with open(in_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)

    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, '') for h in headers])
    wb.save(out_path)
    print(f"{len(rows)} record(s) -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Convert attendance .jsonl to .xlsx")
    parser.add_argument('file', help="Input .jsonl file")
    parser.add_argument('-o', '--output', metavar='FILE', help="Output .xlsx path (default: same name, .xlsx)")
    args = parser.parse_args()

    out_path = args.output or os.path.splitext(args.file)[0] + '.xlsx'
    convert(args.file, out_path)


if __name__ == '__main__':
    main()
