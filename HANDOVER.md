# HANDOVER — Attendance Dry-Run Frontend (FastAPI + Vue)

This document is **self-contained**. You are building a **new, isolated** project (separate
dir, own venv, own monitoring) that **reimplements** the attendance dry-run pipeline — you do
**not** import or call the existing `hik` repo. Everything you need to reimplement it is below.

## What to build

A small internal web app with three features:

1. **Dry-run runner** — form with `start`, `end` (datetime) and optional `employee_no`.
   Runs the pipeline below **in preview mode (nothing is sent to Rymnet)** and shows the
   resolved attendance records in a table.
2. **Download to Excel** — button that exports the dry-run result to `.xlsx`.
3. **Door checklist** — configurable list of doors (toggle active/inactive). The active set
   feeds the dry-run's door filter.

"Dry-run" = fetch + resolve + dedup the records and display them, **without** POSTing to
Rymnet. This is a read-only preview against live Hikvision data.

---

## The pipeline (reimplement this end to end)

```
1. Fetch door events from Hikvision (HMAC-signed POST, paginated)   [live network]
2. For each event: resolve personId -> personCode (HIK person API)  [live network, cache it]
3. Build a record per event (employee_no, logtime, indicator, location)
4. Normalize: employee_no starting "FW" -> "FW-" + rest
5. Mark duplicates (per-person, per-device, within 1 min)
6. If FOREIGN_WORKER filter on: keep only employee_no in Rymnet's category_code=FW roster
7. If employee_no filter given: keep only that employee
8. Drop EXCLUDE_EMPLOYEES from the send list (still show them as audited)
9. DRY RUN: return/stage the records. DO NOT send to Rymnet.
```

Two lists come out of step 3–8:
- **audited** — every record, each tagged `duplicate: true/false`. This is what you show and
  export (the audit trail).
- **deduped** — the subset that *would* be sent (non-duplicate, roster-passing, not excluded).
  In dry-run you don't send it; you can just show a count or highlight.

---

## External API contracts

> **Credentials**: all secrets live in the existing project's `.env`. Copy them into your new
> project's own `.env` (or secret manager). **Never commit them.** Referenced by variable name
> below. Non-secret endpoint paths are shown inline.

### Env vars needed

| Var | Purpose |
|---|---|
| `HIK_APP_KEY` | Hikvision Artemis app key (signing) |
| `HIK_APP_SECRET` | Hikvision Artemis app secret (HMAC key) |
| `HIK_BASE_URL` | e.g. `https://10.1.74.105` (internal, self-signed TLS → disable verify) |
| `HIK_DOOR_EVENTS_PATH` | `/artemis/api/acs/v1/door/events` |
| `HIK_PERSON_PATH` | `/artemis/api/resource/v1/person/personId/personInfo` |
| `RYMNET_TOKEN` | Bearer token for Rymnet (used by the FW-roster lookup) |
| `RYMNET_EMPLOYEE_URL` | `https://api.rymnet.com/RymApi.asmx/GetEmployeeBiodata` |
| `RYMNET_URL` | `https://api.rymnet.com/public/attendance/set` (send endpoint — **not used in dry-run**, listed for completeness) |
| `FOREIGN_WORKER` | `"true"`/`"false"` — enable the FW-roster filter (step 6) |

### 1. Hikvision request signing (REQUIRED for every HIK call)

HIK Artemis uses an HMAC-SHA256 signature over a canonical string. Reimplement exactly:

```python
import hmac, hashlib, base64, time, uuid, json

def build_headers(app_key, app_secret, path, body: str) -> dict:
    timestamp = str(int(time.time() * 1000))
    nonce = uuid.uuid4().hex
    content_md5 = base64.b64encode(hashlib.md5(body.encode()).digest()).decode()
    string_to_sign = "\n".join([
        "POST", "*/*", content_md5, "application/json",
        f"x-ca-key:{app_key}",
        f"x-ca-nonce:{nonce}",
        f"x-ca-timestamp:{timestamp}",
        path,                       # the PATH only, not the full URL
    ])
    signature = base64.b64encode(
        hmac.new(app_secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Content-MD5": content_md5,
        "x-ca-key": app_key,
        "x-ca-timestamp": timestamp,
        "x-ca-nonce": nonce,
        "x-ca-signature-headers": "x-ca-key,x-ca-nonce,x-ca-timestamp",
        "x-ca-signature": signature,
    }
```

Notes:
- `body` must be the **exact** JSON string you POST (the MD5 and signature are over it).
- TLS: base URL is an internal self-signed host → use `verify=False`
  (`urllib3.disable_warnings(...)` to silence warnings).
- The order of headers in `string_to_sign` matters; keep it as-is.

### 2. Door events — `POST {HIK_BASE_URL}{HIK_DOOR_EVENTS_PATH}`

Request body (JSON). Build it, sign it, POST it:

```json
{
  "startTime": "2026-07-01T00:00:00+08:00",
  "endTime":   "2026-07-01T23:59:59+08:00",
  "eventType": 196893,
  "doorIndexCodes": ["4729", "4741"],
  "temperatureStatus": -1,
  "maskStatus": -1,
  "sortField": "SwipeTime",
  "orderType": 0,
  "pageNo": 1,
  "pageSize": 50
}
```

Constants: `eventType=196893` (the access-granted event type we sync), `sortField="SwipeTime"`,
`orderType=0` (ascending — **keep ascending**; dedup relies on chronological order),
`temperatureStatus=-1`, `maskStatus=-1`, `pageSize=50`. Timezone suffix is `+08:00`.
Omit empty string / empty list params (HIK dislikes them).

**Pagination + the 10-door cap** (important):
- `doorIndexCodes` is capped at **10** per request. Split your active door list into chunks of 10.
- For each chunk: POST page 1, read `data.total`, compute `ceil(total/50)` pages, fetch pages
  `1..N`, concatenate `data.list` from every page.
- Response envelope: `{"code": "0", "msg": "...", "data": {"total": <int>, "list": [ ... ]}}`.
  `code != "0"` → treat as error (raise / surface to UI). Each list item has at least
  `personId`, `doorIndexCode`, `eventTime` (ISO string).

### 3. Person info — `POST {HIK_BASE_URL}{HIK_PERSON_PATH}`

Body: `{"personId": "<id>"}` (signed the same way). Response:
`{"code":"0","data":{ "personCode": "...", ... }}`. Use `data.personCode` as the raw
`employee_no`. **Cache by personId** — the same person recurs across events; one lookup each.
On `code != "0"`, treat personCode as empty string and skip (log a warning), don't crash the run.

### 4. Rymnet FW roster (only if `FOREIGN_WORKER=true`) — `GET {RYMNET_EMPLOYEE_URL}`

Query params:
```
access_token = <RYMNET_TOKEN>
format       = Json
filters      = {"category_code":"FW"}     # JSON-encoded string
```
Response is inconsistent: sometimes a list `[{"employee":[...]}]`, sometimes an object
`{"employee":[...]}`. Normalize: if it's a list, take `data[0]` (or `{}` if empty). The roster
is `{ e["employee_no"] for e in data["employee"] }`. Fetch **once per run** and cache.

Same endpoint checks a single employee for the `employee_no` filter: pass
`filters={"employee_no":"<no>"}`; exists if the normalized `employee` list is non-empty.

---

## Record building & transforms (exact logic)

### Record shape

```python
{
  "employee_no": personCode,
  "logtime":     "YYYY-MM-DD HH:MM:SS",   # from eventTime, reformatted (see below)
  "indicator":   "IN" | "OUT" | "",       # from the door
  "location":    doorName,                 # from the door
  "remarks":     "",
}
```
`logtime = datetime.fromisoformat(eventTime).strftime("%Y-%m-%d %H:%M:%S")`.
`indicator`/`location` come from the door config (see Door catalog) keyed by `doorIndexCode`.

### FW prefix normalization

```python
if employee_no.startswith("FW"):
    employee_no = "FW-" + employee_no[2:]
```
(So `FWA00304480` → `FW-A00304480`. Note real Rymnet FW-category employees often have **no**
`FW` prefix at all — that's fine, the roster filter handles them.)

### Deduplication (the subtle part)

Goal: suppress spurious double-reads. Two causes, both handled by one rule:
- **Turn-back**: person walks through a turnstile IN reader, turns their head, and the OUT
  reader on the *same gate* also fires a second later.
- **Double registration**: the same reader fires twice before the person walks through.

Rule: a record is a **duplicate** if, for the **same employee** at the **same physical device**,
another (kept) record occurred **less than 1 minute** earlier.

Key insight — "same physical device" folds the IN and OUT readers of one turnstile together by
**stripping the direction word from the door name**:

```python
import re
def device_id(location: str, indicator: str) -> str:
    # "WHGF TURN IN 1" / "WHGF TURN OUT 1" -> "WHGF TURN 1"
    # "WHCJ IN - FR"   / "WHCJ OUT - FR"   -> "WHCJ - FR"
    d = re.sub(rf"\b{re.escape(indicator)}\b", "", location) if indicator else location
    return " ".join(d.split())
```

Algorithm (process events in ascending time order):

```python
MIN_GAP_SECONDS = 60
last_seen = {}   # (employee_no, device) -> datetime of last KEPT record
audited = []
for rec in records:                       # already resolved + FW-normalized
    emp = rec["employee_no"]
    dev = device_id(rec["location"], rec["indicator"])
    t   = datetime.strptime(rec["logtime"], "%Y-%m-%d %H:%M:%S")
    is_dup = False
    if emp:
        key = (emp, dev)
        if key in last_seen and abs((t - last_seen[key]).total_seconds()) < MIN_GAP_SECONDS:
            is_dup = True                  # duplicate; do NOT update last_seen
        else:
            last_seen[key] = t             # first/kept; window measured from here
    else:
        # empty employee_no fallback: exact (emp, logtime) dedup via a set
        ...
    audited.append({**rec, "duplicate": is_dup})
```

Rules that matter (there are tests for these in the old repo — mirror them):
- Gap is **strict `< 60s`** (exactly 60s apart → **not** a duplicate).
- `last_seen` updates only on kept records (window slides from the last *kept* one, not the dup).
- The **first** of a burst is kept; later ones within 60s drop. Since events are time-ascending
  and the genuine swipe precedes the head-turn artifact, the genuine one survives.
- **Different turnstile number** (e.g. `TURN 1` vs `TURN 2`) → different device → both kept.
  (Assumption: a person doesn't trigger a *different* gate's reader by turning their head.)
- Different employees are independent.

### Filters (apply after dedup marking)

```python
if FOREIGN_WORKER:                 # both lists filtered to the roster
    audited = [r for r in audited if r["employee_no"] in fw_roster]
if employee_no_filter:
    audited = [r for r in audited if r["employee_no"] == employee_no_filter]
deduped = [r for r in audited if not r["duplicate"]
                              and r["employee_no"] not in EXCLUDE_EMPLOYEES]
```

### EXCLUDE_EMPLOYEES (always dropped from the send list, still shown as audited)

```python
EXCLUDE_EMPLOYEES = {
    "RC14338",  # EI EI NYEIN
    "RC14339",  # NYEIN NYEIN EI
    "RC14340",  # CHAN MYAE MON
    "RC14341",  # THAE SU MOE
    "RC14342",  # ZIN MAR MYINT
    "RC15143",  # RAVI MANORANJAN
    "RC15147",  # DARJI AKBAR ALI
    "RC15148",  # KHADKA PRALAD KUMAR
    "RC15477",  # SHUMAN
    "RC9700",   # SAMSINI
}
```

---

## Excel export

Columns = union of keys across records, first-seen order. For dry-run records that's:
`employee_no, logtime, indicator, location, remarks, duplicate`. One header row, one row per
record. Use `openpyxl`:

```python
from openpyxl import Workbook
wb = Workbook(); ws = wb.active
ws.append(headers)
for row in records:
    ws.append([row.get(h, "") for h in headers])
wb.save(path)   # or stream via BytesIO for a FastAPI download response
```

For a download endpoint, write to `io.BytesIO`, then
`StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
headers={"Content-Disposition": 'attachment; filename="attendance.xlsx"'})`.

---

## Door catalog (seed for the checklist)

`doorIndexCode` (int) → door. Only **`type == "Door"`** entries are selectable (Controllers /
CardReaders are infrastructure, never synced). `active: true` = the 6 doors currently synced in
production; the rest are known doors you can enable. Seed your config store (JSON table / DB)
with this, and let the checklist toggle `active`. The dry-run passes the **active** ids as
`doorIndexCodes` (chunked by 10).

```json
[
  {"id": 4729, "name": "WHCJ IN - FR",   "indicator": "IN",  "active": true},
  {"id": 4741, "name": "WHCJ OUT - FR",  "indicator": "OUT", "active": true},
  {"id": 4448, "name": "WHGF TURN OUT 1","indicator": "OUT", "active": true},
  {"id": 4453, "name": "WHGF TURN IN 1", "indicator": "IN",  "active": true},
  {"id": 4458, "name": "WHGF TURN OUT 2","indicator": "OUT", "active": true},
  {"id": 4463, "name": "WHGF TURN IN 2", "indicator": "IN",  "active": true},

  {"id": 1484, "name": "WHGF BIN R IN - FR",     "indicator": "IN",  "active": false},
  {"id": 1578, "name": "HQGF TOILET IN - FR",    "indicator": "IN",  "active": false},
  {"id": 3712, "name": "WHGF INB SC OUT - FR",   "indicator": "IN",  "active": false},
  {"id": 1472, "name": "WHL1 INB MR IN - FR",    "indicator": "IN",  "active": false},
  {"id": 2790, "name": "HQGF PARKING IN - FR",   "indicator": "IN",  "active": false},
  {"id": 4255, "name": "HQL1 TOILET IN - FR",    "indicator": "IN",  "active": false},
  {"id": 1605, "name": "WHL1 OUB MR IN - FR",    "indicator": "IN",  "active": false},
  {"id": 1489, "name": "WHGF BIN R OUT - FR",    "indicator": "OUT", "active": false},
  {"id": 1586, "name": "FR (OUT) GF TO SURAU WOMEN", "indicator": "OUT", "active": false},
  {"id": 1537, "name": "WHGF INB LB 1 OUT - FR", "indicator": "OUT", "active": false},
  {"id": 3707, "name": "WHGF INB SC IN - FR",    "indicator": "OUT", "active": false},
  {"id": 2795, "name": "HQGF PARKING OUT - FR",  "indicator": "OUT", "active": false},
  {"id": 4250, "name": "HQL1 TOILET OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 1525, "name": "WHGF OUB LB 16 OUT - FR","indicator": "OUT", "active": false},
  {"id": 3622, "name": "REST CON ROOM - FR",     "indicator": "IN",  "active": false},
  {"id": 3857, "name": "WHL1 OUB SC IN - FR",    "indicator": "IN",  "active": false},
  {"id": 3978, "name": "HQL2 BCA IN - FR",       "indicator": "IN",  "active": false},
  {"id": 3257, "name": "HQL2 BEH BR IN - FR",    "indicator": "IN",  "active": false},
  {"id": 3414, "name": "HQL2 BIG DOOR IN - FR",  "indicator": "IN",  "active": false},
  {"id": 4475, "name": "HQL2 MIS DOOR IN - FR",  "indicator": "IN",  "active": false},
  {"id": 4492, "name": "HQL2 MAIN GLASS IN - FR","indicator": "IN",  "active": false},
  {"id": 4719, "name": "VIP ENT - FR",           "indicator": "IN",  "active": false},
  {"id": 3918, "name": "WHL2 OUB SC  IN - FR",   "indicator": "IN",  "active": false},
  {"id": 2643, "name": "HQL1 MAIN GLASS IN - FR","indicator": "IN",  "active": false},
  {"id": 2638, "name": "HQL1 MAIN GLASS OUT - FR","indicator": "OUT","active": false},
  {"id": 2648, "name": "HQL1 SMALL DOOR IN - FR","indicator": "IN",  "active": false},
  {"id": 2653, "name": "HQL1 SMALL DOOR OUT - FR","indicator": "OUT","active": false},
  {"id": 2616, "name": "HQL2 TOILET IN - FR",    "indicator": "IN",  "active": false},
  {"id": 2621, "name": "HQL2 TOILET OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 4182, "name": "GH EXT - FR",            "indicator": "OUT", "active": false},
  {"id": 3402, "name": "GH ENT - FR",            "indicator": "IN",  "active": false},
  {"id": 2599, "name": "HQGF SMALL DOOR IN - FR","indicator": "IN",  "active": false},
  {"id": 2604, "name": "HQGF SMALL DOOR OUT - FR","indicator": "OUT","active": false},
  {"id": 3245, "name": "HQGF MAIN DOOR IN - FR", "indicator": "IN",  "active": false},
  {"id": 3852, "name": "WHL1 OUB SC OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 3973, "name": "HQL2 BCA OUT - FR",      "indicator": "OUT", "active": false},
  {"id": 3262, "name": "HQL2 BEH BR OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 3419, "name": "HQL2 BIG DOOR OUT - FR", "indicator": "OUT", "active": false},
  {"id": 4480, "name": "HQL2 MIS DOOR OUT - FR", "indicator": "OUT", "active": false},
  {"id": 4497, "name": "HQL2 MAIN GLASS OUT - FR","indicator": "OUT","active": false},
  {"id": 3913, "name": "WHL2 OUB SC OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 2431, "name": "HQL2 SERVER IN - FR",    "indicator": "IN",  "active": false},
  {"id": 1501, "name": "WHGF OUB SC IN - FR",    "indicator": "IN",  "active": false},
  {"id": 1600, "name": "WHGF OUB LB 7 OUT - FR", "indicator": "OUT", "active": false},
  {"id": 1506, "name": "WHGF OUB SC OUT - FR",   "indicator": "OUT", "active": false},
  {"id": 4824, "name": "ADM IN - FR",            "indicator": "IN",  "active": false},
  {"id": 4829, "name": "ADM OUT - FR",           "indicator": "OUT", "active": false},
  {"id": 4998, "name": "FR (OUT) GF TO SURAU WOMEN", "indicator": "OUT", "active": false}
]
```

> The source repo also lists Controller/CardReader entries (not shown — they're not doors and
> never sync). If you ever need them they're in the old `DoorList.py`, but you shouldn't.

---

## Suggested backend shape (FastAPI)

- `POST /api/dry-run` → body `{start, end, employee_no?}`. Runs the pipeline against the
  **active** doors. Returns `{records: [...audited...], sent_count, dup_count}`.
  - The run does live HIK calls (events + one person lookup per unique personId) and, if
    `FOREIGN_WORKER`, one Rymnet roster call. A wide date range over many doors can take a
    while → consider a **background job + polling** (`POST` returns a job id, `GET
    /api/dry-run/{id}` returns status/result) rather than a long blocking request. For a
    single employee / short range, synchronous is fine.
- `GET /api/dry-run/{id}/xlsx` (or `POST /api/export` with the records) → streams the `.xlsx`.
- `GET /api/doors` → the catalog with `active`. `PUT /api/doors` → persist toggles.
- Keep a store for door config (SQLite/JSON). This is your source of truth, not any Python file.

## Suggested frontend (Vue)

- **Dry-run view**: date-range pickers + optional employee input + "Run" → results table with a
  `duplicate` column (highlight dup rows) and a summary (`sent_count` / `dup_count`) → "Download
  Excel" button.
- **Doors view**: checklist (grouped by site prefix like `WHGF`, `WHCJ`, `HQ...` is a nice touch)
  with active toggles + Save.

---

## Gotchas / invariants (don't lose these)

- **Ascending event order is load-bearing** for dedup (`orderType=0`). Sort by logtime if your
  source ever returns them unordered.
- **1-minute gap is strict `<`**; device key folds IN/OUT of the same gate; different gate
  numbers stay distinct.
- **HIK TLS is self-signed** → `verify=False`.
- **doorIndexCodes cap = 10** → chunk.
- **personCode cache** — don't re-lookup the same personId.
- Dry-run **must not** POST to `RYMNET_URL`. The only Rymnet call in dry-run is the read-only
  FW-roster GET (and only when `FOREIGN_WORKER=true`).
- Secrets stay in env, never in the repo or this doc.

---

## Open decisions (confirm with the requester)

1. **Door config store** — SQLite vs flat JSON file? (Recommend SQLite if you'll add more config later.)
2. **Auth** — internal LAN, no login (simplest) vs a shared password? 
3. **Dry-run execution** — sync response vs background job + poll? (Depends on typical date range breadth.)
4. **Where does this app run** — it needs LAN access to the Hikvision box (`10.1.74.105`). Same host as the existing scheduler, or another LAN machine?
