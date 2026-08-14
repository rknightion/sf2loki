---
id: doc-0002
title: Wave operating model
type: guide
created_date: '2026-08-14 17:01'
updated_date: '2026-08-14 17:01'
---
> This document carries **only what is specific to sf2loki**. The campaign model itself — run
> contract and run modes, the routing contract, authority and the thread pool, child lane briefs,
> external-contract freezing, the unattended blocker contract, the goal-file template and the
> pre-flight checklist — lives in the **Agent fan-out protocol (canonical)** document and is
> deliberately not restated here. A summary that drifts from its source while looking authoritative
> is the exact defect this split exists to prevent.
>
> Read both before designing a wave. `backlog doc list --plain` shows them.

## What the tracker changes about a run

Three deltas against the canonical model, and they are properties of the tracker rather than of the
campaign model:

1. **The goal file no longer enumerates the work.** It keeps the framing — run contract, lanes,
   ownership, routing, constraints, traps — and the queue becomes a query:
   `backlog task list --plain -m "<milestone>" -s "To Do"`. One source of truth for what is left,
   and it survives the run.
2. **The run-end report is not a file.** Task state is the record: landed work is `Done` with the
   commit SHA in its final summary, blocked work is `Parked` with a concrete resume boundary,
   untouched work is self-evidently still `To Do`, discovered work is a new task labelled
   `needs-triage`. The closing message goes to the terminal as a covering note answering *what did
   this run learn that no single task captures* — and nothing durable may live only there.
3. **Acceptance criteria live in the task.** All 71 imported tasks already carry theirs. A lane
   reads its own contract with `backlog task view <id> --plain`; the goal file does not restate it.

Finalize in one call, so an interrupted lane cannot leave finished work looking unfinished:

```bash
backlog task edit SFL-0007 --check-ac 1 --check-ac 2 -s Done
```

## Rules this project added, and the failure behind each

**Never pin a bug.** Tests pin *current intended* behaviour. If writing a test reveals the
production code is actually wrong, stop, land the fix, then pin post-fix behaviour. This exists
because the test-coverage milestone overlaps the correctness milestone in five known places —
SFL-0029↔SFL-0009, SFL-0057↔SFL-0012/SFL-0015, SFL-0054↔SFL-0045/SFL-0046, SFL-0027↔SFL-0014,
SFL-0056↔SFL-0016 — and a lane that pinned the buggy behaviour first would have made the correctness
fix look like a regression.

**Deterministic synchronization only in tests — no real-time sleeps.** Established when flaky
timing tests cost a wave; use the fake-clock and event patterns already in `tests/conftest.py` and
the coordinate/state helpers.

**Validate the premise before implementing.** Every imported task was written against a `file:line`
snapshot of `main` on 2026-07-30 and those references drift. Re-confirm against current code first;
if the premise is wrong or the work already landed, say so in the task's notes and close it — never
force a change to match a stale finding.

**No breaking default change without a decision recorded first.** Hardening that could break a live
deployment (compose `read_only`, a ServiceAccount automount flip, a Helm default) lands with the
changelog line spelled out in the task. This is why SFL-0071 is fixed by making the docs claim
*true* rather than by weakening the claim.

**Coverage numbers are evidence, not the goal.** No task closes on "module now at N%"; each closes
on its named behaviours being pinned.

**These findings are what the scanners miss.** The repo already runs zizmor, CodeQL, actionlint,
dependency-review, Scorecard and docker-security in CI. Do not "fix" a security task by re-running
a scanner and reporting it clean.

**Feature work is not done without its docs page.** And, where the feature has an operator-visible
failure mode, without `doctor` support for it.

## The recurring defect classes in this codebase

Every one of these has multiple concrete instances in the board. A lane that knows the class will
recognise the next instance; treat them as the review checklist for any change in the area.

**A checkpoint advancing past data that was never delivered.** The single most expensive class here,
because Loki ingest is not correctable after the fact. Instances: SFL-0002 (a shutdown-abandoned
batch leapfrogged by the next same-key commit), SFL-0009 (a transient body-download failure loses
the log body *and* advances past it), SFL-0045/SFL-0046 (s3/gcs conditional-write retry paths),
SFL-0049 (backfill checkpoints ignore `state.store` entirely), SFL-0012/SFL-0014/SFL-0015 (lease
epoch fences regressing a shared checkpoint store). **Rule of thumb: commit-after-push is the
invariant; any code path that commits without having pushed is guilty until it proves otherwise.**

**Silent starvation or permanent stall, with readiness still reporting ready.** SFL-0004 (only one
of two budget-pause waiters sees the UTC-day rollover; the other lane stalls ~24h), SFL-0042 (a
throttled discovery doesn't seed the cycle's gate, so the tail of the type list starves),
SFL-0011 (an unbounded `Id NOT IN` halts a big object permanently), SFL-0043, SFL-0003.

**Observability that lies to the operator.** The reason the Grafana milestone exists at all.
SFL-0006 (idle gauges stop exporting and `noDataState: Ok` hides it), SFL-0022 (rules querying
windows EventLogFile timestamps can never occupy — they can never fire), SFL-0023/SFL-0024
(dashboards extracting fields the underlying event does not have), SFL-0021 (an alert that breaches
on every normal ELF drain), and the doctor family — SFL-0017, SFL-0018, SFL-0032, SFL-0035 — where
a preflight check passes on something that is broken. **A green check that cannot go red is a
defect, not a passing test.**

**Multi-org identity that is absent or leaks across orgs.** SFL-0016, SFL-0025, SFL-0026, SFL-0044,
SFL-0056. Static labels take precedence over per-entry labels, so anything set deployment-wide in
multi-org mode overwrites the per-org identity `OrgSource` injects — and Loki bakes labels into
streams at ingest, so the misattribution is permanent.

**A config surface that accepts nonsense and fails much later.** SFL-0047 (a blank secret counts as
present and shadows `*_file`), SFL-0053 (`token_ttl` below the refresh skew re-mints on every API
call), SFL-0050 (`--event-types` bypasses the guard config enforces, and `*` silently backfills
nothing), SFL-0044.

## Lane conventions and contention

**`src/sf2loki/app.py` is the contention point of this entire board: 40 of the 71 tasks touch it.**
`src/sf2loki/config.py` is second at 38. One file = one owner is not a formality here — funnel every
`app.py` edit in a wave through a single lane, and never run two `config.py` lanes concurrently.
Where a wave genuinely needs both, sequence them and make the `app.py` lane last so it absorbs the
seam changes.

**Generated artefacts are a shared resource, not a file each.** `just gen-config` rewrites
`config.example.yaml`, `docs/config-reference.md` and the Helm values block together, and
`tests/test_config_artifacts_drift.py` is a CI gate. Two lanes regenerating concurrently produce a
conflicting diff every time. One lane regenerates, at the end, after the config changes have landed.

**Frozen seams stay frozen.** The `Source` / `Sink` / `CheckpointStore` / `Coordinator` protocols in
`src/sf2loki/*/base.py` do not change; new sources, sinks, stores and coordinators implement them.
The Loki label allowlist (`sinks/loki/labels.py:ALLOWED_LABELS`) does not grow — structured metadata
is the escape hatch. Either/or-per-event-category stands. Single-instance-by-default stands: new
coordinators extend HA, they do not enable scale-out.

**Exclusive resources — at most one lane at a time, and the wave must schedule this explicitly:**

- **The DEV Salesforce org behind `.env.dev`.** One org, one API budget, one set of Pub/Sub
  subscriptions and replay positions. Two lanes doing live validation against it will exhaust the
  daily API limit, cross-consume each other's events, and produce results neither can trust.
- **The Grafana dev stack** (gcx context `rkaidev`). Pushing dashboards or rules mutates shared
  state; a snapshot taken while another lane is pushing is meaningless.
- **`deploy/grafana/**`** — hand-authored, one resource per file, no generator and no drift gate.
  Nothing catches a bad edit here except a human looking at a snapshot.

## Ownership, and the escape hatch

A lane owns its files, its task, and the decision of whether its premise still holds. It does not
own: the frozen seams, the label allowlist, a breaking default, or anything outside its task's
acceptance criteria.

**The escape hatch: a lane that hits a decision its task does not cover STOPS and returns the
question rather than inventing an answer.** One round-trip is cheaper than the rewrite. A boundary
with no escape hatch is a stop condition wearing a safety label — so this one is explicit: returning
a question is a successful lane outcome, not a failure, and the task goes to `Parked` with the
question as its resume boundary.

**Sub-agents never commit.** Only the orchestrating session commits, and it commits straight to
`main` and pushes immediately, with a conventional-commit type that release-please can read
(`fix:` / `feat:` / `docs:` / `test:` / `ci:`). Bypass-on-push output is expected and is not a
problem to report.

## Run-end against this tracker

At the end of a run, before the covering note:

```bash
backlog task list --plain -s "In Progress"    # must be empty — nothing is left mid-flight
backlog task list --plain -s Parked           # each must carry a concrete resume boundary
backlog task list --plain -l needs-triage     # work discovered during the run
backlog milestone list --plain                # the wave's completion, in one line
```

Anything still `In Progress` is a bug in the run, not a status. Resolve it to `Done` or `Parked`
before the run ends.

## Two things that are true of this repo and surprise people

**There is no `#NN` any more.** This repo's history — commit messages, code comments, `AGENTS.md`,
the docs site — cites GitHub issue numbers heavily, and those issues were deleted on 2026-08-14. The
numbers still resolve, but against `archive/issues-dump.json`, not against GitHub:
`jq '.[] | select(.number == 86)' archive/issues-dump.json`. Do not "fix" a stale `#NN` reference by
guessing at a task ID; look it up. The closed-work index document maps the closed set.

**SFL-0001 is the one task that belongs to no milestone.** It predates the 2026-07-30 audit wave, so
it is in no epic's phase plan. That is accurate, not an import error — do not file it under a
milestone to tidy the board.
