# Loop: sf2loki
tier: guarded
gate: just check
ci-required: ci-success
grafana-stack: rkaidev

`backlog/` is committed: no org ID, instance URL, tenant ID, email address or token in any task or doc;
write the shape, not the instance. Run `just` with stdin from `/dev/null`.

## Credentials

- The DEV Salesforce org credentials sit in the gitignored `.env.dev`; the Grafana dev stack is gcx
  context `rkaidev`. Both belong to the root: a lane does no live validation.

## Traps

- Never pin a bug: a test pins current intended behaviour. If writing one shows production code is
  wrong, land the fix first, then pin post-fix behaviour.
- Tests use deterministic synchronization (the fake-clock and event patterns in `tests/conftest.py`),
  never real-time sleeps.
- Validate a task's premise against current code before implementing; its `file:line` references
  drift. If the work already landed or the premise is wrong, say so in the task notes and close it.
- No breaking default change (compose `read_only`, a ServiceAccount automount flip, a Helm default)
  without a recorded decision and the changelog line spelled out.
- Coverage numbers are evidence, not the goal: a task closes on named behaviours pinned. A security
  task is not fixed by re-running a scanner (zizmor, CodeQL, actionlint, Scorecard already run in CI).
- A feature is not done without its docs page and, for an operator-visible failure mode, `doctor`
  support.
- Commit-after-push is the invariant: any path that advances a checkpoint without having pushed is
  guilty until proven otherwise (Loki ingest is not correctable after the fact). Readiness can report
  ready through a silent stall, and a check that cannot go red is a defect.
- Static labels take precedence over per-entry labels, so a deployment-wide label in multi-org mode
  overwrites the per-org identity from `OrgSource`, permanently once Loki bakes it into the stream.
- Config accepts nonsense and fails late: a blank secret counts as present and shadows `*_file`;
  `token_ttl` below the refresh skew re-mints on every API call.
- Frozen seams: the `Source`, `Sink`, `CheckpointStore` and `Coordinator` protocols in
  `src/sf2loki/*/base.py`, and the Loki label allowlist (`ALLOWED_LABELS`, where structured metadata
  is the escape hatch). Single-instance-by-default stands.
- `#NN` in history is a deleted GitHub issue; resolve it with
  `jq '.[] | select(.number == 86)' archive/issues-dump.json`, never by guessing a task ID.

## Mutexes

- `src/sf2loki/app.py` and `src/sf2loki/config.py` are the contention points: one owning lane each,
  never two `config.py` lanes at once, and the `app.py` lane goes last so it absorbs seam changes.
- `just gen-config` rewrites `config.example.yaml`, `docs/config-reference.md` and the Helm values
  block together (`tests/test_config_artifacts_drift.py` gates it): one lane regenerates, at the end.
- The DEV Salesforce org (one API budget, one set of Pub/Sub replay positions), the Grafana dev stack
  and `deploy/grafana/**` (hand-authored, no drift gate) are exclusive, root only.
