---
id: SFL-0067
title: >-
  helm: don't automount the ServiceAccount API token unless the pod actually
  talks to the API server
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/151'
ordinal: 67000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The Helm chart never sets `automountServiceAccountToken`, so Kubernetes applies its default (`true`) and projects a live, auto-refreshed ServiceAccount API token into every sf2loki pod at `/var/run/secrets/kubernetes.io/serviceaccount` — including the default single-instance install, which has no use for it.

- `deploy/helm/templates/deployment.yaml:47` sets `serviceAccountName:`; the pod spec (deployment.yaml:46-100) sets `securityContext`, affinity, topology spread, tolerations and `priorityClassName` but never `automountServiceAccountToken`.
- `deploy/helm/templates/rbac.yaml:7-19` creates the ServiceAccount with only name/namespace/labels/annotations, so it does not suppress the mount at the SA level either.
- `deploy/helm/values.yaml:41-52` exposes `serviceAccount.{create,name,annotations,esoName}` and no automount knob, so an operator cannot opt out through values.
- `grep -rn automount` over the repo returns nothing: chart, docs, CI and tests are all silent on the field.

The token has exactly one legitimate consumer, and it is HA-gated:

- `src/sf2loki/coordinate/k8s_lease.py:461` calls `config.load_incluster_config()` (only when `coordinate.k8s_lease.kubeconfig` is unset), which reads the projected token and CA bundle.
- `src/sf2loki/doctor.py:678` does the same inside `_default_k8s_api_factory`, reached only from `_check_coordinator_k8s_lease` for a configured `k8s_lease` coordinator.

No other code in `src/` reads `/var/run/secrets` or contacts `kubernetes.default.svc`. Chart defaults are `ha.enabled: false` (values.yaml:18-19), `config.coordinate.type: noop` (values.yaml:650) and `networkPolicy.enabled: false` (values.yaml:195), so the default render mounts an API credential that no code path uses and no policy constrains.

The chart already documents that this topology needs no control-plane access. `deploy/helm/templates/networkpolicy.yaml` puts the API-server egress rule inside `{{- if .Values.ha.enabled }}` with the comment "Omitted entirely when ha.enabled is false so a non-HA deployment doesn't need control-plane reachability at all" — yet the credential for that control plane stays mounted in exactly that topology.

Nothing pins the current behaviour. The `helm-chart` CI job (`.github/workflows/ci.yml:118-174`) runs `helm lint`, three `helm template | kubeconform` permutations (ci.yml:162-165) and one render-guard assertion (ci.yml:167-174); kubeconform checks schema conformance and passes either way. `tests/test_config_artifacts_drift.py` is the only test touching `deploy/helm`, and it checks generated values drift, not pod-security fields.

## Why it matters

sf2loki parses attacker-influenceable input: decoded Pub/Sub Avro payloads, SOQL result rows, EventLogFile CSVs and ApexLog bodies. A pod compromise through any of those paths currently hands the attacker a valid, kubelet-refreshed API-server token. Even with no RBAC bound to the ServiceAccount (the non-HA case renders no Role or RoleBinding — rbac.yaml:34-75 is entirely inside `{{- if .Values.ha.enabled }}`), that token authenticates as a member of `system:authenticated` and `system:serviceaccounts`, which grants API discovery, `SelfSubjectRulesReview`/`SelfSubjectReview`, `TokenRequest` probing, and whatever else a cluster has granted those groups — a common misconfiguration in shared clusters. With `networkPolicy.enabled: false` by default, nothing blocks the egress to the API server either.

This is defense-in-depth rather than a directly exploitable hole, which is why the severity is low. It is also a standard benchmark item that scanners flag on this chart today (CIS Kubernetes Benchmark 5.1.6, Checkov `CKV_K8S_38`, kube-score `pod-probes`/SA checks), and it is the one remaining gap in an otherwise thoroughly hardened pod spec: `readOnlyRootFilesystem`, `allowPrivilegeEscalation: false`, all capabilities dropped, `RuntimeDefault` seccomp, `runAsNonRoot` (values.yaml:162-174), and a `resourceNames`-scoped namespaced Role (rbac.yaml:55-58).

## Proposed approach

Derive the value rather than hardcoding it, because the token IS required for `k8s_lease`. Gating on `.Values.ha.enabled` alone is too narrow: `config.coordinate.type: k8s_lease` can legitimately be set with `ha.enabled: false` (single replica, or externally managed RBAC via `serviceAccount.create: false`), and when `configOverride` is non-empty the template cannot see the coordinator type at all — the same blind spot the single-instance render guard at deployment.yaml:8 already handles by skipping itself.

1. Add a helper to `deploy/helm/templates/_helpers.tpl` (alongside `sf2loki.serviceAccountName` at _helpers.tpl:48-55):

   ```gotemplate
   {{/* Mount the SA API token only when something in the pod actually calls the API
        server: the k8s_lease coordinator (coordinate/k8s_lease.py load_incluster_config)
        is the only consumer. A raw configOverride hides the coordinator type from the
        template, so default to true there (operator owns correctness, same as the
        single-instance render guard). serviceAccount.automount overrides everything —
        set it true for an extraContainers sidecar that needs API access. */}}
   {{- define "sf2loki.automountServiceAccountToken" -}}
   {{- if not (kindIs "invalid" .Values.serviceAccount.automount) -}}
   {{- .Values.serviceAccount.automount -}}
   {{- else if .Values.configOverride -}}
   true
   {{- else if or .Values.ha.enabled (eq (dig "coordinate" "type" "noop" .Values.config) "k8s_lease") -}}
   true
   {{- else -}}
   false
   {{- end -}}
   {{- end -}}
   ```

2. In `deploy/helm/templates/deployment.yaml`, add to the pod spec next to `serviceAccountName` (line 47):

   ```yaml
   automountServiceAccountToken: {{ include "sf2loki.automountServiceAccountToken" . }}
   ```

3. Mirror it on the created ServiceAccount in `deploy/helm/templates/rbac.yaml` (inside the `serviceAccount.create` block, lines 7-19) so any other pod referencing the SA inherits the same default. The pod-spec field takes precedence, so the two agreeing is intentional.

4. Add the knob to `deploy/helm/values.yaml` under `serviceAccount:` (lines 42-52):

   ```yaml
     # null (default) = derived: the API token is mounted only when the pod talks to the
     # API server (ha.enabled, config.coordinate.type: k8s_lease, or a raw configOverride
     # where the chart cannot tell). Set true when an extraContainers sidecar needs API
     # access, false to force it off.
     automount: null
   ```

5. Extend the `helm-chart` CI job with a render assertion next to the existing render-guard step (`.github/workflows/ci.yml:167-174`), asserting `automountServiceAccountToken: false` in the default render and `true` in the HA, `coordinate.type=k8s_lease`, `configOverride` and explicit-override renders. Also add `--set config.coordinate.type=k8s_lease` as a fourth `validate` case (ci.yml:162-165) so that permutation is schema-checked.

6. Document it in `deploy/helm/README.md` and `docs/deployment/kubernetes.md`, and note in `docs/deployment/high-availability.md` (near line 78, where the lease RBAC is described) that HA requires the mounted token. Include the workload-identity caveat: EKS IRSA injects its own projected volume at `/var/run/secrets/eks.amazonaws.com/serviceaccount` via the pod-identity webhook and is unaffected by `automountServiceAccountToken: false`, and GKE Workload Identity resolves ADC through the node metadata server, so the S3/GCS state-store paths that rely on `serviceAccount.annotations` (values.yaml:46-48) keep working. Verify both against a live render before asserting it in docs.

---

Imported from GitHub issue #151 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 151)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki.automountServiceAccountToken` helper added to `deploy/helm/templates/_helpers.tpl` with the precedence order: explicit `serviceAccount.automount` > non-empty `configOverride` > `ha.enabled` or `config.coordinate.type == k8s_lease` > false.
- [ ] #2 `deploy/helm/templates/deployment.yaml` pod spec sets `automountServiceAccountToken` from the helper.
- [ ] #3 `deploy/helm/templates/rbac.yaml` created ServiceAccount sets the same value.
- [ ] #4 `deploy/helm/values.yaml` documents `serviceAccount.automount: null` with the derivation and the sidecar escape hatch.
- [ ] #5 `helm template sf2loki deploy/helm` (defaults) renders `automountServiceAccountToken: false` on both the Deployment pod spec and the ServiceAccount.
- [ ] #6 `helm template ... --set ha.enabled=true --set replicaCount=2` renders `true`.
- [ ] #7 `helm template ... --set config.coordinate.type=k8s_lease` (with `ha.enabled=false`) renders `true`.
- [ ] #8 `helm template ... --set configOverride='service: {}'` renders `true`.
- [ ] #9 `helm template ... --set serviceAccount.automount=true` renders `true` on an otherwise-default install, and `--set serviceAccount.automount=false --set ha.enabled=true --set replicaCount=2` renders `false` (explicit override wins both ways).
- [ ] #10 `.github/workflows/ci.yml` `helm-chart` job asserts each of the five renders above (grep-based step in the style of the existing render-guard assertion at ci.yml:167-174) and adds `--set config.coordinate.type=k8s_lease` to the kubeconform `validate` permutations.
- [ ] #11 `deploy/helm/README.md`, `docs/deployment/kubernetes.md` and `docs/deployment/high-availability.md` document the behaviour, including the verified IRSA / GKE Workload Identity note that `automountServiceAccountToken: false` does not break the S3/GCS state-store credential path.
- [ ] #12 `just gate` green (`ruff` + `mypy --strict` + `pytest`), and `helm lint deploy/helm` clean.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
