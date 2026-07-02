# CLI Reference

The `sf2loki` executable (also runnable as `python -m sf2loki`) has one top-level command that
runs the daemon, plus subcommands for setup, preflight, and operations. All subcommands accept
`--config PATH` (default: env vars + built-in defaults, no file).

```console
$ sf2loki --help
```

## Global options

| Flag | Description |
|---|---|
| `--version` | Print the installed version and exit. |
| `--config PATH` | YAML config file. Env vars override file values; built-in defaults apply if omitted entirely. |
| `--check` | Validate config and wiring (secrets resolve, labels are legal, sources don't overlap) with no network calls, then exit. Only applies when no subcommand is given. |

## Exit codes

`--check`, `run` (no subcommand), and `backfill` all return **2** for the same class of failure —
a config or wiring problem (bad secrets, source overlap, org selection) — so scripts can check for
one code regardless of entrypoint. `doctor` also returns 2 if the config itself can't be
loaded/selected, but uses exit **1** for a live-preflight check FAIL on an otherwise-valid config.

| Code | Meaning |
|---|---|
| `0` | Success (or `--check`/`doctor` passed with no FAIL). |
| `1` | `doctor` found at least one FAIL. `backfill` failed persistently (10 consecutive Loki push failures). |
| `2` | Config/wiring error — bad secrets, invalid YAML, source overlap, bad org selection. |

## `sf2loki` (run the daemon)

No subcommand starts the long-running ingestion service: load config, build sources/sink/state,
run until a shutdown signal.

```bash
sf2loki --config config.yaml
```

## `sf2loki --check`

Validates configuration and wiring **offline** — secrets resolve, Loki labels are legal, sources
don't overlap — and never touches the network. Exits `0` (ok) or `2` (invalid).

```bash
sf2loki --check --config config.yaml
```

## `sf2loki doctor`

Live end-to-end preflight: authenticates for real, checks the integration user's permissions,
probes Pub/Sub topic reachability, reports which configured EventLogFile types the org has
produced files for, and pushes exactly **one** test line to Loki (labelled
`source=sf2loki-doctor`) to confirm the write path — that push is the only write `doctor` ever
performs. Prints a PASS/WARN/FAIL table and exits `1` if anything FAILed, `0` otherwise (WARNs are
fine). Checks run in dependency order; a check whose dependency FAILed reports SKIP instead of
failing confusingly.

| Flag | Description |
|---|---|
| `--json` | Emit machine-readable JSON instead of the table (for CI). |
| `--org NAME` | For a multi-org config, which org to check (default: the first configured org). Ignored for single-org configs. |

```bash
sf2loki doctor --config config.yaml
```

See [Troubleshooting](../troubleshooting.md) for how to read specific WARN/FAIL rows.

## `sf2loki backfill`

One-shot, resumable historical EventLogFile backfill into Loki. Uses its own checkpoint file (a
`-backfill` sibling of the daemon's state file), so it's safe to run alongside the running service.

| Flag | Default | Description |
|---|---|---|
| `--since DATE` | *(required)* | Start of the backfill window, `YYYY-MM-DD` (UTC, inclusive). |
| `--until DATE` | now | End of the backfill window, `YYYY-MM-DD` (UTC, exclusive). |
| `--event-types LIST` | configured types | Comma-separated ELF EventTypes to backfill. |
| `--interval {Daily,Hourly}` | `Daily` | Which ELF interval to backfill (Daily is complete per Salesforce docs). |
| `--ingest-timestamps` | off | Push at ingest time instead of true event time (event time preserved in structured metadata key `event_time`); skips the default `backfill="true"` label strategy. |
| `--concurrency N` | `2` | Concurrent file downloads (each spools up to 8 MiB). |
| `--org NAME` | first configured org | For a multi-org config, which org to backfill. Ignored for single-org configs. |

```bash
sf2loki backfill --config config.yaml --since 2026-05-01 --until 2026-06-01 \
  --event-types Login,API --interval Daily
```

Prints a summary on exit (files processed, rows pushed/dropped, bytes pushed, API calls used,
elapsed time).

## `sf2loki config example|reference|schema`

Prints generated configuration documentation to stdout — no config file needed.

| `kind` | Output |
|---|---|
| `example` | Annotated example YAML (same content as [`config.example.yaml`](https://github.com/rknightion/sf2loki/blob/main/config.example.yaml)). |
| `reference` | Markdown reference of every key, type, default, and description (same content as [config-reference.md](../config-reference.md)). |
| `schema` | The config's JSON schema. |

```bash
sf2loki config example > config.yaml
```

## `sf2loki state show|set|delete`

Inspects and repairs checkpoints in the configured state store. See the
[state runbook](../deployment/state.md) for stuck-watermark recovery walkthroughs.

All three subcommands share `--force`, which bypasses the file store's exclusive lock — unsafe if
the daemon is still running against the same state file.

### `state show`

Pretty-prints checkpoints from the configured store. Nothing is redacted.

| Flag | Default | Description |
|---|---|---|
| `--key GLOB` | `*` | `fnmatch` glob to filter checkpoint keys. |
| `--force` | off | Bypass the exclusive lock. |

```bash
sf2loki state show --config config.yaml --key 'pubsub:*'
```

### `state set`

CAS-safe write of a single checkpoint key.

```bash
sf2loki state set --config config.yaml 'pubsub:/event/LoginEventStream' '{"replay_id": "..."}'
```

### `state delete`

Removes a checkpoint key so its source restarts from its preset/lookback.

```bash
sf2loki state delete --config config.yaml 'pubsub:/event/LoginEventStream'
```
