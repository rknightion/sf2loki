# src/sf2loki/state

The `CheckpointStore` seam (`base.py`: `load` / `commit`, keyed per source stream) with three
backends, plus `org_view.py` for multi-org key namespacing.

## Every backend is a compare-and-swap, by a different primitive

- `file_store.py` (the default): atomic tmp-then-rename, plus an exclusive `flock` on a
  `<state file>.lock` sidecar held for the process's lifetime, so a second instance pointed at the
  same file fails fast with `StateFileLockError` instead of double-ingesting. `build_store` passes
  `exclusive_lock=False` when a real coordinator is configured: the lease is the exclusivity
  mechanism there, cross-host flock is meaningless, and an old leader that is dead to the lease but
  still alive on the box would crash-loop the newly promoted one. A corrupt or truncated file
  raises `StateFileCorruptError` naming the recovery step, never silently discarded.
- `s3_store.py` (needs the `sf2loki[s3]` extra): the whole document at one bucket/key, committed
  with `If-Match` on the ETag (`If-None-Match: *` for the first write), so a concurrent writer gets
  `StateStoreConflictError` rather than clobbering.
- `gcs_store.py` (needs the `sf2loki[gcs]` extra): same whole-document shape, but the CAS is a GCS
  generation precondition (`ifGenerationMatch: "0"` first, then the current generation). It imports
  `StateStoreConflictError` from `s3_store` rather than redefining it. Load is `download_metadata`
  (for the generation) then `download` (for the body), and the `upload` kwarg is `parameters=`, not
  `params=`.

Both object stores import their client library lazily inside the default client factory, so the
modules stay importable and unit-testable with an injected fake and no extra installed.
`build_store` therefore has to check the extra itself, and for GCS it checks the bare top-level
name `find_spec("gcloud")`: `find_spec("gcloud.aio.storage")` imports the parent and RAISES when
the extra is absent, bypassing the friendly error.

## Fencing hooks are duck-typed, not on the protocol

All three stores implement `set_fence(...)`; only `file_store.py` implements `set_epoch(...)`, the
durable epoch fence that gives the CAS-less shared file store what the object stores get from an
ETag or a generation. `app.py` wires whichever the active coordinator offers via `getattr`. Neither
is declared on the `CheckpointStore` protocol. `../coordinate/AGENTS.md` owns the leadership
contract behind them.

## Multi-org key namespacing (`org_view.py`)

`OrgCheckpointView` prefixes every key with `org=<name>:` so two orgs sharing one store never
collide. The FIRST configured org additionally falls back to the unprefixed legacy key on a load
miss, so a deployment upgraded from single-org to multi-org resumes from its existing state and
migrates forward on the next commit. That fallback is the migration path, not dead code.
