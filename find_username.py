import argparse
import json
import os
import sys
from signature.door_events import iter_events
from signature.personId import fetch_person_info
from DoorList import DoorList

TIMEZONE = '+08:00'
DOORS = [str(k) for k, v in DoorList.items() if v['type'] == 'Door']
EVENT_TYPE = 196893


def main():
    parser = argparse.ArgumentParser(description="Search Hikvision door events by employee")
    parser.add_argument('-t', nargs=2, metavar=('START', 'END'), required=True,
                        help="e.g. 2026-06-01T08:00:00 2026-06-01T18:00:00")
    parser.add_argument('-u', metavar='EMPLOYEE', default=None,
                        help="Employee name or person ID (partial, case-insensitive); omit for all events")
    args = parser.parse_args()

    start, end = args.t
    if '+' not in start and 'Z' not in start:
        start += TIMEZONE
    if '+' not in end and 'Z' not in end:
        end += TIMEZONE

    search = args.u.lower() if args.u else None
    results = []
    person_cache = {}

    # suppress the debug print(body) in door_events._fetch_page
    devnull = open(os.devnull, 'w')
    old_stdout = sys.stdout
    sys.stdout = devnull

    try:
        for event in iter_events(
            start_time=start,
            end_time=end,
            event_type=EVENT_TYPE,
            person_name='',
            person_id='',
            person_code='',
            door_index_codes=DOORS,
            temperature_status=-1,
            mask_status=-1,
            sort_field='SwipeTime',
            order_type=1,
        ):
            pid = event.get('personId', '')
            if pid not in person_cache:
                try:
                    info = fetch_person_info(pid)
                    person_cache[pid] = {
                        'personCode': info.get('personCode', ''),
                        'personName': info.get('personName', ''),
                    }
                except RuntimeError:
                    person_cache[pid] = {'personCode': '', 'personName': ''}
            event['_resolved'] = person_cache[pid]

            if search is None:
                results.append(event)
            else:
                name = str(event.get('personName', '')).lower()
                code = str(person_cache[pid]['personCode']).lower()
                rname = str(person_cache[pid]['personName']).lower()
                if search in name or search in code or search in rname or search in pid.lower():
                    results.append(event)
    finally:
        sys.stdout = old_stdout
        devnull.close()

    output = "No data found" if not results else json.dumps(results, indent=2, ensure_ascii=False)
    with open('find_results.log', 'w', encoding='utf-8') as f:
        f.write(output + '\n')
    print(f"{'No data found' if not results else str(len(results)) + ' record(s)'} → find_results.log")


if __name__ == '__main__':
    main()
