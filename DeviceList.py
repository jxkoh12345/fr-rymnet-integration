# FR terminals read directly over ISAPI, keyed by device IP.
#
# Same fields as DoorList, plus the device address and the Artemis doorIndexCode
# the device corresponds to (kept for cross-checking against the server-side
# pipeline). Every mapping below was confirmed by event parity: the device's own
# log and Artemis' door/events API returned identical (employee_no, timestamp)
# sets for these doors — 1457 events over 3 days, zero difference.
#
# Doors listed here are commented out of DoorList so the Artemis path no longer
# fetches them; they are served by the device path instead (see main.run_device_cycle).

DeviceList = {
    '10.1.72.122': {"doorIndexCode": 4729, "type": "Door", "doorName": "WHCJ IN - FR",  "indicator": "IN"},
    '10.1.72.119': {"doorIndexCode": 4741, "type": "Door", "doorName": "WHCJ OUT - FR", "indicator": "OUT"},
}
