---
id: SFL-0017
title: >-
  cli: doctor never probes the real state file, its .lock sidecar, or the lease
  file - an unreadable one passes preflight and crash-loops the daemon
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/101'
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`doctor`'s file-backend state check and file-lease coordinator check validate only the *parent directory*, never the files the daemon actually opens.

`_check_state_file` (`src/sf2loki/doctor.py:498-530`) computes `state_dir = file_cfg.path.parent`, creates/writes/flocks/unlinks a fresh probe file `.sf2loki-doctor-probe` (`doctor.py:71`) inside it, and returns `PASS` with `state directory {state_dir} is writable and lockable` (`doctor.py:530`). `file_cfg.path` is never stat'd or opened. `_check_coordinator_file_lease` (`doctor.py:609-637`) has the identical shape for `cfg.path.parent` and `.sf2loki-doctor-coordinator-probe` (`doctor.py:81`), returning `PASS` with `lease directory {lease_dir} is writable`.

The daemon opens three real paths the probes never touch:

| path | opened at | mode needed |
| --- | --- | --- |
| `state.file.path` | `state/file_store.py:143` (`self._path.read_text()` in `_read_file_fresh`) | read |
| `<state.file.path>.lock` sidecar | `state/file_store.py:125` (`os.open(lock_path, O_RDWR\|O_CREAT, 0o644)`) | read+write |
| `coordinate.file_lease.path` | `coordinate/file_lease.py:338` (`self._path.read_text()` in `_read`) | read |

`_read_file_fresh` catches only `json.JSONDecodeError`/`UnicodeDecodeError` (`file_store.py:144`); `PermissionError` propagates. Nothing upstream handles it — `cli.py:219` and `cli.py:264` catch `ConfigError`/`ValueError` only — so the daemon dies with a bare traceback and a non-zero exit (container restart loop), with none of the actionable uid-10001 guidance that `config.py:1486-1493` produces for unreadable secret files or that `doctor.py:512-521` produces for an unwritable state directory.

The lease path fails differently and more quietly: `FileLease._read` converts `PermissionError` into `_LeaseReadError` (`coordinate/file_lease.py:341-342`), and `_acquire` treats that as "can't tell whether a holder is live", logs `cannot read file lease; backing off without contesting` and re-polls forever (`file_lease.py:188-199`). The replica never becomes leader, so ingestion never starts at all — no crash, no FAIL, just a permanently passive process.

Reproduced against current `main` (state directory 0700-writable, real file unreadable):

```
DOCTOR:  state PASS  state directory /tmp/... is writable and lockable
DAEMON:  PermissionError [Errno 13] Permission denied: '/tmp/.../state.json'

DOCTOR2: state PASS  state directory /tmp/... is writable and lockable   # state.json 0644, .lock mode 000
DAEMON2: PermissionError [Errno 13] Permission denied: '/tmp/.../state.json.lock'
```

## Why it matters

The precondition is not exotic — the code guarantees the real state file ends up mode 0600 owned by whichever uid wrote it last. `_flush` writes through `tempfile.mkstemp` (`state/file_store.py:170`), which creates 0600 regardless of umask, then `os.replace` promotes that temp file to the real path (`file_store.py:179`). `FileLease._write` does the same (`coordinate/file_lease.py:370`). Verified: after one `commit()` the resulting `state.json` is `0600` owned by the writer.

Concrete failure walk:

1. The container (uid 10001) runs normally and writes `/var/lib/sf2loki/state.json` as `0600 10001`. The host directory is `chmod 770` + `chown 10001` exactly as `docs/troubleshooting.md:38` and `docs/installation.md:63` instruct.
2. A watermark wedges. The operator follows the documented repair runbook (`sf2loki state set|delete`, `cli.py:167-188`, `docs/deployment/state.md`) host-side under `sudo`. `_flush` replaces the file, so `state.json` is now `0600 root:root` — still inside a directory writable by uid 10001.
3. `sf2loki doctor` → `state PASS state directory /var/lib/sf2loki is writable and lockable`.
4. The container restarts and crash-loops on `PermissionError` reading the checkpoint file doctor just certified. Writes would have been fine (tmp + rename needs directory permission only), so nothing but the read of the real file could have caught it.

Same false negative for a `0600`-by-another-uid lease file under `coordinate.type: file_lease`, where the symptom is a silently leaderless deployment rather than a crash. This is the preflight class `doctor` exists for, and its own FAIL string at `doctor.py:516-521` already warns about this exact permissions family — it just never inspects the files where it bites.

Existing tests pin only the directory behaviour: `tests/test_doctor.py:536` (probe file cleaned up) and `tests/test_doctor.py:555` (chmod-000 *directory* FAILs). The "deliberately NOT the real state file" rule at `doctor.py:69-73` is about never flocking or writing the live file — a non-locking read check does not violate it.

Not covered by #59, whose fix dispatched the state check on `state.store` and added the telemetry/coordinator probes while explicitly preserving the probe-not-the-real-object rule.

## Proposed approach

Extend both file-backend probes with a non-mutating check of the real paths, run *after* the existing directory probe so a missing directory still reports the current message. Never flock and never write these files — a live daemon holds the sidecar flock and may be renewing the lease.

1. `_check_state_file` (`doctor.py:498`): after the directory probe, for each of `file_cfg.path` and `file_cfg.path.with_name(file_cfg.path.name + ".lock")` that exists:
   - `FAIL` when it is not a regular file (`Path.is_file()` false while `exists()` is true — a directory at that path raises `IsADirectoryError` in `_read_file_fresh`).
   - For the state file, require read access: `os.access(path, os.R_OK)`, or the stronger `os.close(os.open(path, os.O_RDONLY))` inside `try/except PermissionError`. Write access is *not* required (commits go through tmp + `os.replace`, needing directory permission only) — do not FAIL on a read-only state file.
   - For the `.lock` sidecar, require read+write: `os.open(path, os.O_RDWR)` then close immediately, with no `flock` call.
   - `FAIL` message must name the offending path, distinguish it from the directory case, and carry the same remediation shape as `doctor.py:516-521`: the service runs as uid 10001, so `chmod 640`/`chown 10001` the file (a root-owned `chmod 0600` state file crash-loops the container), and note that `sf2loki state set|delete` run as another user rewrites the file with that user's ownership.
   - A non-existent state file stays `PASS` (first run; `_read_file_fresh` returns `{}` at `file_store.py:140-141`).
2. `_check_coordinator_file_lease` (`doctor.py:609`): same treatment for `cfg.path` — regular-file + readable, no write, no flock. Its FAIL text should state the consequence: an unreadable lease makes every replica back off forever (`file_lease.py:188-199`) and no instance ever ingests.
3. Widen the `PASS` details so they say what was actually verified, e.g. `state directory ... is writable and lockable; state.json and state.json.lock are readable` vs the current directory-only wording.
4. Optional, same change set: document in `docs/deployment/state.md` that `state`-subcommand repairs performed as a different user leave the state file owned by that user (`file_store.py:170` + `:179`), and that the fix is `chown 10001 state/state.json`.

---

Imported from GitHub issue #101 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 101)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_check_state_file` FAILs when `state.file.path` exists but is unreadable by the current uid, and the message names the file (not just the directory) plus the chmod/chown remediation.
- [ ] #2 `_check_state_file` FAILs when `<state.file.path>.lock` exists but is not open-able `O_RDWR` by the current uid.
- [ ] #3 `_check_state_file` FAILs when `state.file.path` exists and is not a regular file (e.g. a directory).
- [ ] #4 `_check_state_file` still PASSes when `state.file.path` does not exist (first run) and when it exists readable.
- [ ] #5 Neither probe calls `flock` on the real `.lock` file, and neither writes to `state.file.path` or the lease path — a doctor run while the daemon is live leaves both files byte-identical and the daemon unaffected.
- [ ] #6 `_check_coordinator_file_lease` FAILs when `coordinate.file_lease.path` exists but is unreadable, with text naming the leaderless-forever consequence.
- [ ] #7 Tests in `tests/test_doctor.py` beside `test_state_dir_permission_denied_fails` (`tests/test_doctor.py:555`): `test_state_file_unreadable_fails`, `test_state_lock_sidecar_unreadable_fails`, `test_state_file_not_a_regular_file_fails`, `test_state_file_absent_still_passes`, `test_coordinator_lease_file_unreadable_fails` — each creating the real file under `tmp_path`, `chmod 0o000`, restoring perms in a `finally` so `tmp_path` cleanup succeeds.
- [ ] #8 A regression test proving doctor and the daemon agree: a `state.json` written by `FileCheckpointStore.commit` then `chmod 0o000` makes `doctor`'s state row FAIL, where `FileCheckpointStore.load` raises `PermissionError` on the same path.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
