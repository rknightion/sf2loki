# CLAUDE.md — src/sf2loki/state

The `CheckpointStore` seam (`base.py`: `load`/`commit`, keyed per source
stream) with three backends — `file_store.py` (local JSON, the default),
`s3_store.py` (S3-compatible object storage, needs the `sf2loki[s3]` extra),
and `gcs_store.py` (Google Cloud Storage, needs the `sf2loki[gcs]` extra) —
plus `org_view.py` for multi-org key namespacing. See `../CLAUDE.md` and
`../coordinate/CLAUDE.md` for how leadership fencing plugs into this seam.

## Two backends, same conditional-write idea
- `FileCheckpointStore`: atomic tmp-then-rename writes; a `flock`ed
  `<state file>.lock` sidecar held for the process's lifetime prevents a
  second instance pointed at the same file from double-ingesting and
  clobbering checkpoints (fails fast with `StateFileLockError`, not silently).
  A corrupt/truncated file raises `StateFileCorruptError` naming the recovery
  step — never silently discarded.
- `S3CheckpointStore`: the whole checkpoint document lives at one bucket/key;
  commits use conditional writes (`If-Match` ETag, or `If-None-Match: *` for
  the first write) so a concurrent writer fails fast (`StateStoreConflictError`)
  instead of clobbering — the object-store analogue of the file store's flock.
  `aiobotocore` is imported lazily inside the default client factory, so this
  module stays importable (and unit-testable with an injected fake client)
  without the `s3` extra installed.
- `GcsCheckpointStore`: the GCS analogue of the S3 store — same whole-document
  + conditional-write shape, but the CAS uses GCS **generation preconditions**
  (`ifGenerationMatch: "0"` for the first write, the current generation for an
  update) instead of an ETag, and the fail-fast conflict raises the same
  `StateStoreConflictError` (imported from `s3_store`, not redefined). Load is
  `download_metadata` (for the generation) + `download` (for the body).
  `gcloud-aio-storage` is lazily imported in the default client factory (needs
  the `gcs` extra); the `upload` kwarg is `parameters=`, not `params=`.
  `build_store` guards it with `find_spec("gcloud")` — the bare top-level name,
  since `find_spec("gcloud.aio.storage")` would *raise* when the extra is absent.
- All three stores expose an optional `set_fence(...)` duck-typed hook consumed by
  `coordinate/file_lease.py` — not part of the `CheckpointStore` Protocol
  itself, since only the file-lease coordinator needs it (`NoopCoordinator`
  never calls it).

## Multi-org key namespacing (`org_view.py`)
`OrgCheckpointView` prefixes every key with `org=<name>:` so two orgs sharing
one store never collide. The **first** configured org additionally falls back
to the unprefixed legacy key on a load miss, so a deployment upgraded from
single-org to multi-org resumes from its existing state file and migrates
forward transparently on the next commit — don't "fix" that fallback away, it
is the migration path, not dead code.
