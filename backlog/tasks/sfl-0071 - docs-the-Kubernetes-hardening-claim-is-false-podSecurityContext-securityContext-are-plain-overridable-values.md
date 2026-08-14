---
id: SFL-0071
title: >-
  docs: the Kubernetes hardening claim is false -
  podSecurityContext/securityContext are plain overridable values
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/155'
ordinal: 71000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`docs/deployment/kubernetes.md:143-145` promises the container hardening is immutable:

> The container runs as uid/gid `10001`, `readOnlyRootFilesystem: true`, all capabilities dropped, `seccompProfile: RuntimeDefault` — none of this is configurable away, it ships hardened by default.

The chart does not honour that. Both security contexts are ordinary chart values:

- `deploy/helm/values.yaml:162-168` — `podSecurityContext` (`runAsNonRoot: true`, `runAsUser/runAsGroup/fsGroup: 10001`, `seccompProfile.type: RuntimeDefault`)
- `deploy/helm/values.yaml:169-174` — `securityContext` (`readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`)

rendered verbatim with no filtering or merge base:

- `deploy/helm/templates/deployment.yaml:52-53` — `securityContext:` / `{{- toYaml .Values.podSecurityContext | nindent 8 }}`
- `deploy/helm/templates/deployment.yaml:130-131` — `securityContext:` / `{{- toYaml .Values.securityContext | nindent 12 }}`

There is no enforcement anywhere: the chart's only `fail` is the single-instance guard at `deploy/helm/templates/deployment.yaml:9` (`replicaCount>1` without `ha.enabled`); `deploy/helm/` has no `values.schema.json`; and the CI helm job (`.github/workflows/ci.yml:118-174`) lints, renders three permutations through kubeconform, and asserts only the replicaCount guard fires.

Verified by rendering the chart as shipped:

```
helm template t deploy/helm \
  --set podSecurityContext.runAsNonRoot=false \
  --set podSecurityContext.runAsUser=0 \
  --set-json 'securityContext={"privileged":true}'
```

renders `runAsNonRoot: false`, `runAsUser: 0` and `privileged: true` with no warning or render failure. `--set securityContext.readOnlyRootFilesystem=false` likewise renders `readOnlyRootFilesystem: false`.

The docs compound the problem by never naming the two keys: grep for `securityContext` across `docs/` and `deploy/helm/README.md` returns nothing outside the sentence above, so the false immutability statement is the only guidance a reader gets on the subject.

## Why it matters

An operator hitting a perceived permission problem (or pasting a values snippet from another chart) sets `securityContext.readOnlyRootFilesystem=false` or `podSecurityContext.runAsUser=0`, the chart renders it happily, and the workload silently runs with weaker isolation than the published documentation asserts is impossible. The gap is also the kind a compliance or security review takes at face value: the sentence reads as a structural guarantee about the artifact, and the artifact does not provide one. Not an insecure default and not remotely exploitable — the defect is the false claim plus the absence of any signal when the hardening is stripped.

## Proposed approach

Two parts; the doc correction is mandatory, the guard is the mechanism that keeps the corrected wording true over time.

1. **Correct the doc sentence** (`docs/deployment/kubernetes.md:143-145`). State the real contract: ships hardened by default (uid/gid 10001, `readOnlyRootFilesystem: true`, all capabilities dropped, `seccompProfile: RuntimeDefault`), overridable via the `podSecurityContext` / `securityContext` values, and name the values so the override path is discoverable instead of accidental. Note that overriding `readOnlyRootFilesystem` does not remove the need for the `/tmp` + state-dir `emptyDir` mounts (`deploy/helm/templates/deployment.yaml:159`).

2. **Add a render guard for the non-negotiable inversions**, matching the existing single-instance guard pattern at `deploy/helm/templates/deployment.yaml:9`. Fail the render when the effective contexts contain any of:
   - `securityContext.privileged: true`
   - `securityContext.allowPrivilegeEscalation: true`
   - `podSecurityContext.runAsNonRoot: false` or `podSecurityContext.runAsUser: 0`

   with a single documented escape hatch value (e.g. `unsafeAllowPrivilegedSecurityContext: false` in `deploy/helm/values.yaml`) that bypasses the guard for the rare cluster that genuinely needs it. The `fail` message should name the value the operator set and the escape hatch. Benign relaxations (`readOnlyRootFilesystem: false`, a different non-zero uid, added capabilities other than a privileged escalation) stay allowed — the guard covers root and privilege escalation only, so it does not turn a normal Helm override surface into a locked one.

Do not hardcode the whole context into the template: charts are expected to expose these values, and removing the override surface breaks clusters that need a different uid/fsGroup for their storage class.

---

Imported from GitHub issue #155 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 155)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `docs/deployment/kubernetes.md` no longer claims the hardening is not configurable away; it states the defaults, that they are the shipped default, and names `podSecurityContext` / `securityContext` as the override values.
- [ ] #2 `deploy/helm/README.md` documents both values with their defaults and the guard's escape hatch.
- [ ] #3 `deploy/helm/templates/deployment.yaml` fails the render for `privileged: true`, `allowPrivilegeEscalation: true`, `runAsNonRoot: false`, and `runAsUser: 0` unless the escape-hatch value is set; the `fail` message names the offending field and the escape hatch.
- [ ] #4 `helm template sf2loki deploy/helm` (defaults) still renders and still passes the existing kubeconform validation in `.github/workflows/ci.yml`.
- [ ] #5 `helm template sf2loki deploy/helm --set securityContext.readOnlyRootFilesystem=false` still renders successfully (benign relaxation is not blocked).
- [ ] #6 `.github/workflows/ci.yml` gains an assertion step mirroring "Assert single-instance render guard fires" (`ci.yml:167-174`) that each of the four inversions fails the render, and that the render succeeds when the escape-hatch value is set.
- [ ] #7 A pytest case (alongside `tests/test_config_artifacts_drift.py`) asserts `deploy/helm/values.yaml` still carries the hardened defaults — `podSecurityContext.runAsNonRoot: true`, `runAsUser: 10001`, `runAsGroup: 10001`, `fsGroup: 10001`, `seccompProfile.type: RuntimeDefault`, `securityContext.readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop == ["ALL"]` — so a future values edit that silently weakens the shipped default fails the gate.
- [ ] #8 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
