---
id: SFL-0070
title: >-
  helm: lease RBAC silently binds to the namespace `default` ServiceAccount when
  serviceAccount.create=false with no name
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/154'
ordinal: 70000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki.serviceAccountName` (`deploy/helm/templates/_helpers.tpl:49-55`) falls back to the literal string `"default"` when `serviceAccount.create` is false and `serviceAccount.name` is empty:

```gotemplate
{{- define "sf2loki.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sf2loki.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
```

With `ha.enabled: true`, `deploy/helm/templates/rbac.yaml:69` interpolates that helper straight into the lease RoleBinding subject. Verified render:

```
$ helm template sf2loki deploy/helm --set ha.enabled=true --set replicaCount=2 \
    --set serviceAccount.create=false
...
kind: RoleBinding
metadata:
  name: sf2loki-lease
subjects:
  - kind: ServiceAccount
    name: default          # <-- the namespace's shared default SA
    namespace: default
roleRef:
  kind: Role
  name: sf2loki-lease
```

The Role being bound (`deploy/helm/templates/rbac.yaml:44-58`) grants:

- `create` on **all** `coordination.k8s.io/leases` in the namespace, deliberately unscoped because RBAC `resourceNames` cannot gate `create` (rbac.yaml:41-43 documents why);
- `get` + `update` scoped via `resourceNames` to `ha.leaseName` (default `sf2loki-leader`).

The chart already knows this SA is the wrong subject. The `rbac.yaml:2-5` header comment gives the shared-`default`-SA hazard as the stated reason the chart creates its own SA by default: *"Created by default so the chart doesn't depend on a pre-existing 'default' SA (which may carry unrelated permissions in a shared namespace)"*. The `create: false` path then binds to exactly that SA with no complaint.

Nothing catches the combination:

- No render guard. The only `fail` in the chart is the single-instance guard at `deploy/helm/templates/deployment.yaml:8-10`.
- No `values.schema.json` exists in `deploy/helm/`, so `serviceAccount.name` cannot be conditionally required.
- No test. `tests/` has no Helm-render test at all.
- CI never exercises it. The three `validate` permutations in `.github/workflows/ci.yml` (default, `--set ha.enabled=true --set replicaCount=2 --set networkPolicy.enabled=true`, and the externalSecrets one) all leave `serviceAccount.create` at its `true` default.
- No doc warning. `deploy/helm/values.yaml:42-53` says only "Defaults to the chart fullname when empty", which is true for `create: true` and silent about the `create: false` fallback; `deploy/helm/README.md` has no ServiceAccount section beyond the ESO `serviceAccount.esoName` note at README.md:166-171.

## Why it matters

The misconfiguration is **completely silent** — sf2loki itself keeps working. `deploy/helm/templates/deployment.yaml:47` uses the same helper, so the pod also runs as `serviceAccountName: default` and holds the permissions it needs. Nothing fails, nothing logs, and the operator has no signal.

The damage is blast radius. Every other pod in the namespace that omits `serviceAccountName` also runs as `default`, so an unrelated workload inherits:

- `get` + `update` on the sf2loki leader Lease — enough to overwrite `holderIdentity`/`renewTime` and seize or thrash leadership. Seizing it drives sf2loki into standby (ingest stops); flapping it produces repeated failovers, and a leadership change is exactly the condition the epoch/commit fence work (#47, #48) exists to bound.
- `create` on arbitrary Leases in the namespace — a compromised pod can pre-create a Lease under a name another controller's leader election is about to use, interfering with that controller.

A compromised or hostile co-tenant pod in a shared namespace is the realistic path; the operator's intent (`create: false` to bind an externally-managed IRSA/workload-identity SA) is defeated silently rather than loudly.

## Proposed approach

Fail the render on the ambiguous combination, mirroring the existing single-instance guard's shape. In `deploy/helm/templates/rbac.yaml`, before the `{{- if .Values.ha.enabled }}` block at line 36:

```gotemplate
{{- if and .Values.ha.enabled (not .Values.serviceAccount.create) (not .Values.serviceAccount.name) -}}
{{- fail "ha.enabled with serviceAccount.create=false requires an explicit serviceAccount.name: leaving it empty resolves to the namespace's shared `default` ServiceAccount, and the lease RoleBinding would grant create-on-any-Lease plus get/update on the leader Lease to every pod in the namespace that does not set serviceAccountName. Set serviceAccount.name to your externally-managed SA, or leave serviceAccount.create=true." -}}
{{- end -}}
```

Failing beats defaulting here: silently substituting the fullname would bind a Role to an SA that does not exist (the pod then cannot start), and there is no safe guess for an externally-managed SA's name.

Supporting changes:

- Document the fallback and the requirement in the `serviceAccount` block of `deploy/helm/values.yaml:42-53` — state that with `create: false`, an empty `name` means the namespace `default` SA, and that HA rejects it.
- Add a ServiceAccount subsection to `deploy/helm/README.md` covering `create: false` and the `name` requirement.
- Extend the CI permutations in `.github/workflows/ci.yml`: add a `validate --set ha.enabled=true --set replicaCount=2 --set serviceAccount.create=false --set serviceAccount.name=sf2loki-external` positive case, and a negative assertion in the style of the existing "Assert single-instance render guard fires" step that `helm template ... --set ha.enabled=true --set replicaCount=2 --set serviceAccount.create=false` exits non-zero.

Consider whether `externalSecrets.enabled` with an empty `serviceAccount.esoName` deserves the same treatment — `deploy/helm/templates/externalsecret.yaml:25-26,36-37` renders an empty `serviceAccountRef.name`, and `deploy/helm/README.md:164-166` calls `esoName` required without enforcing it. Out of scope here unless bundled deliberately.

---

Imported from GitHub issue #154 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 154)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `helm template sf2loki deploy/helm --set ha.enabled=true --set replicaCount=2 --set serviceAccount.create=false` exits non-zero with a message naming `serviceAccount.name` and the `default` SA hazard.
- [ ] #2 `helm template sf2loki deploy/helm --set ha.enabled=true --set replicaCount=2 --set serviceAccount.create=false --set serviceAccount.name=sf2loki-external` renders successfully, with the RoleBinding subject and the Deployment `serviceAccountName` both `sf2loki-external`.
- [ ] #3 `helm template sf2loki deploy/helm --set serviceAccount.create=false` (HA off) still renders successfully and emits no Role/RoleBinding — the guard must not fire outside HA, since without lease RBAC the `default` SA gains nothing from this chart.
- [ ] #4 Default install and the existing `ha.enabled=true --set replicaCount=2` permutation are unchanged (guard does not fire when `serviceAccount.create` is true).
- [ ] #5 No rendered RoleBinding in any chart permutation names `default` as a subject.
- [ ] #6 `.github/workflows/ci.yml` gains the positive `serviceAccount.create=false --set serviceAccount.name=...` HA `validate` permutation and a negative step asserting the new guard fires, matching the existing single-instance-guard assertion step.
- [ ] #7 `deploy/helm/values.yaml` `serviceAccount` block documents that `create: false` with an empty `name` resolves to the namespace `default` SA and is rejected under `ha.enabled`.
- [ ] #8 `deploy/helm/README.md` documents the `serviceAccount.create: false` + `name` requirement.
- [ ] #9 `helm lint deploy/helm` passes.
- [ ] #10 `just gate` green (no Python change expected; confirm no drift gate trips).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
