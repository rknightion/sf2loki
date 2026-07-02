# Synthetic Activity Tool

`scripts/generate_activity.py` drives constant, randomised activity through the Salesforce REST
API so a dev org has realistic data for EventLogFile ingestion and the streaming pipeline to chew
on. It's a **dev/test-only** utility — it only ever touches the org named by `SF_LOGIN_URL`, and
has no place in a production deployment.

## What it generates

Covers the breadth of event categories a real org sees:

- object CRUD (Account, Contact, Lead, Opportunity, Case, Task, Event) — `API`/`RestApi` ELF
  events, plus the underlying records product queries hit
- SOQL queries — `API` ELF events
- SOSL searches — `Search` ELF events
- Bulk API 2.0 ingest **and** query jobs — `BulkApi` ELF events
- composite / batch requests — `API` ELF events
- sObject + global describe calls — `API` (metadata) ELF events
- file upload + download (ContentVersion) — `ContentTransfer` / file-transfer ELF events
- anonymous Apex via the Tooling API — `ApexExecution` ELF events
- periodic re-authentication and token revocation — `Login` / `Logout` ELF events plus
  `LoginEventStream`

**Capability-gated**: a one-time `discover()` probe at startup checks which of these the org
actually has, logs the active set, and silently skips the rest so the tool stays reliable in any
org:

- report runs, synchronous and async — `Report` / `AsyncReportRun` ELF events
- dashboard reads — `Dashboard` ELF events
- Campaign + CampaignMember, and Contract records — more object coverage

Actions are picked from a weighted menu each tick, so the traffic pattern looks organic rather
than lock-step. `URI`, `LightningPageView`, and `LightningInteraction` stay out of reach — see
[Caveats](#caveats).

## Data pools

To keep the tenant looking real across long runs, account/company and person values are drawn from
large committed pools rather than a handful of hard-coded names:

- `scripts/data/companies.csv` — `name,industry,website,description` (industry is a valid
  Salesforce picklist value; hundreds of fictional companies spanning regions and all standard
  industries).
- `scripts/data/people.csv` — `first_name,last_name,title` (hundreds of fictional people).

Records draw a base name from these pools and often get a short random suffix (a number or letter
code) appended; emails get a random numeric tag — so repeat runs keep minting fresh-looking
records instead of colliding. If the CSVs are missing, the script falls back to a small built-in
pool, so it always runs. Creates also send `Sforce-Duplicate-Rule-Header: allowSave=true`, so
Salesforce duplicate-alert rules never hard-fail a create.

## Prerequisites

Same JWT-bearer credentials sf2loki itself uses — the script authenticates with the identical flow
(see [Salesforce setup](https://github.com/rknightion/sf2loki/blob/main/README.md#salesforce-setup-oauth)).
It reads these from a `.env`-style file or the environment:

- `SF_LOGIN_URL`
- `SF_CLIENT_ID`
- `SF_USERNAME`
- `SF_PRIVATE_KEY_FILE`
- `SF_API_VERSION` (optional; also settable via `--api-version`)

`.env.dev` (gitignored, the same file the docker-compose setup uses) is a convenient place to keep
these.

## Usage

```bash
# continuous low-level noise (~6 ops/min) until Ctrl-C
python scripts/generate_activity.py --env-file .env.dev

# heavier burst for an hour
python scripts/generate_activity.py --env-file .env.dev --ops-per-min 30 --duration 3600

# delete everything this tool ever created
python scripts/generate_activity.py --env-file .env.dev --cleanup
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--env-file PATH` | none | Load `KEY=VALUE` env vars from this file (e.g. `.env.dev`). |
| `--ops-per-min N` | `6` | Approximate operations per minute. |
| `--duration N` | `0` | Run for N seconds then stop (`0` = until Ctrl-C). |
| `--relogin-every N` | `420` | Re-authenticate every N seconds (produces `Login` events). |
| `--api-version VER` | `60.0` (or `SF_API_VERSION`) | Salesforce API version. |
| `--cleanup` | off | Delete all records this tool created, then exit. |
| `--verbose` / `-v` | off | Debug logging. |

## Cleanup marker

Every record the script creates gets `[sf2loki-synthetic]` stamped into its `Description` field.
`--cleanup` walks each object type it can create (Task, Event, Case, Opportunity, Contact, Lead,
Account — children first so parents delete cleanly) and deletes every marked record via the
sObject Collections API, so a run can always be fully undone without hand-picking records.

`Description` is a Long Text Area, which SOQL **cannot** filter on (`WHERE Description LIKE …`
raises `INVALID_FIELD`), so cleanup `SELECT`s the `Description` and matches the marker
client-side. This is also why it only ever deletes records this tool created — real or sample
records in the org (which don't carry the marker) are never touched.

## Caveats

!!! warning "Dev/test tool only"
    This script performs real writes (and deletes, under `--cleanup`) against whatever org
    `SF_LOGIN_URL` points at. Never point it at a production org.

- **Cases are never closed.** Closing a Case fires Salesforce's post-close Survey email flow,
  which would spam the org user's mailbox. The generator creates and works/escalates Cases but
  deliberately has no "close case" action.
- **UI-only ELF types can't be generated via API.** `URI`, `LightningPageView`, and
  `LightningInteraction` EventLogFile types require a real browser UI session — there's no
  REST/Bulk API path that produces them, so this script can't cover them.
