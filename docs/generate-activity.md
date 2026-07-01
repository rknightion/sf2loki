# Synthetic activity generator

`scripts/generate_activity.py` drives constant, randomised activity through the Salesforce REST
API so a dev org has realistic data for EventLogFile ingestion and the streaming pipeline to chew
on. It's a dev/test utility — it only ever touches the org named by `SF_LOGIN_URL`.

## What it generates

Covers the breadth of event categories a real org sees:

- object CRUD (Account, Contact, Lead, Opportunity, Case, Task, Event) — `API`/`RestApi` ELF events,
  and the underlying records product queries hit
- SOQL queries — `API` ELF events
- SOSL searches — `Search` ELF events
- report runs (existing reports in the org) — `Report` ELF events
- Bulk API 2.0 ingest jobs — `BulkApi` ELF events
- anonymous Apex via the Tooling API — `ApexExecution` ELF events
- periodic re-authentication — `Login` ELF events + `LoginEventStream`

Actions are picked from a weighted menu each tick (creates, field updates, opportunity stage
advances, queries, searches, report runs, Apex, bulk inserts), so the traffic pattern looks organic
rather than lock-step.

## Prerequisites

Same JWT-bearer credentials sf2loki itself uses — the script authenticates with the identical flow
(see [Salesforce setup (OAuth)](../README.md#salesforce-setup-oauth)). It reads these from a
`.env`-style file or the environment:

- `SF_LOGIN_URL`
- `SF_CLIENT_ID`
- `SF_USERNAME`
- `SF_PRIVATE_KEY_FILE`
- `SF_API_VERSION` (optional; also settable via `--api-version`)

`.env.dev` (gitignored, same file the docker-compose setup uses) is a convenient place to keep
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
| `--env-file PATH` | none | load `KEY=VALUE` env vars from this file (e.g. `.env.dev`) |
| `--ops-per-min N` | `6` | approx operations per minute |
| `--duration N` | `0` | run for N seconds then stop (`0` = until Ctrl-C) |
| `--relogin-every N` | `420` | re-authenticate every N seconds (produces `Login` events) |
| `--api-version VER` | `60.0` (or `SF_API_VERSION`) | Salesforce API version |
| `--cleanup` | off | delete all records this tool created, then exit |
| `--verbose` / `-v` | off | debug logging |

## Cleanup marker

Every record the script creates gets `[sf2loki-synthetic]` stamped into its `Description` field.
`--cleanup` queries each object type it can create (Account, Contact, Lead, Opportunity, Case,
Task, Event) for that marker and deletes every match via the sObject Collections API — so a run
can always be fully undone without hand-picking records.

## Caveats

- **Cases are never closed.** Closing a Case fires Salesforce's post-close Survey email flow, which
  would spam the org user's mailbox. The generator creates and works/escalates Cases but
  deliberately has no "close case" action.
- **UI-only ELF types can't be generated via API.** `URI`, `LightningPageView`, and
  `LightningInteraction` EventLogFile types require a real browser UI session — there's no REST/Bulk
  API path that produces them, so this script can't cover them.
