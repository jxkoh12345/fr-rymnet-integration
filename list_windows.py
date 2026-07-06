"""List state/windows/ files with their window range and pending status.

Filenames are signature hashes, not readable — this prints the actual
start/end/page/pending for each so you can find the one you want.

Usage:
  uv run list_windows.py
"""
import glob
import json
import os

_WINDOWS = os.path.join('state', 'windows', '*.json')


def main():
    files = sorted(glob.glob(_WINDOWS))
    if not files:
        print("No window files in state/windows/.")
        return
    for path in files:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        q = data.get('query', {})
        pending = 'YES' if data.get('pending') else 'no'
        print(f"{os.path.basename(path)}: {q.get('start')} -> {q.get('end')}  page={data.get('page')}  pending={pending}")


if __name__ == '__main__':
    main()
