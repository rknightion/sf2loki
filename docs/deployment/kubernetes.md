# Kubernetes

sf2loki ships a proper Helm chart at
[`deploy/helm/`](https://github.com/rknightion/sf2loki/tree/main/deploy/helm),
published as an **OCI chart** on every release. It replaces the old hand-applied
example manifests (`deploy/k8s/`, now removed) with a single install/upgrade path
that renders the ServiceAccount, Deployment, Service, PodDisruptionBudget, and
(when enabled) the HA lease RBAC and NetworkPolicy.

## Prerequisites

- Kubernetes >= 1.21 (the chart's floor is `policy/v1` PodDisruptionBudget).
- Helm 3.x or later with OCI registry support (built in since Helm 3.8).
- A Salesforce connected app (JWT bearer or client_credentials) and a Loki push
  endpoint — see [Configuration](../configuration/index.md) if you haven't set
  these up yet.

## Quick install (single instance)

The chart is published to GHCR as an OCI artifact and cosign-signed by the
shared container-publish pipeline:

```bash
helm show values oci://ghcr.io/rknightion/charts/sf2loki --version 0.2.0
```

Write a `my-values.yaml` with your Salesforce/Loki config (see
[Configuration](#configuration) below), then install:

```bash
helm install sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
  --version 0.2.0 \
  -n sf2loki --create-namespace \
  -f my-values.yaml
```

This installs a **single replica**, `coordinate.type: noop`, and
`state.store: file` on an emptyDir — fine for one instance, but the state is
lost if the pod is rescheduled onto a different node. Point `state.store` at
`s3`/`gcs` even for a single-instance deployment if you want checkpoints to
survive a pod replacement; it's required for HA regardless (see below).

## Configuration

Values pass through two independent knobs:

- **`config:`** — a generated template of the full config schema (same shape
  as `config.docker.yaml`), with the Pydantic model defaults and comments
  baked in. Pick `config.salesforce` **or** `config.orgs` (exactly one — the
  values file ships both for reference) and fill in the empty required fields:
  login URL/environment, `client_id`, `username` (JWT bearer) or
  `auth_mode: client_credentials`, and the Loki `sink.loki.url` /
  `tenant_id`. `*_file` fields point at `/etc/sf2loki/secrets/…` — see
  [Secrets](#secrets) for how those files get mounted.
- **`configOverride:`** — a raw YAML string that, when non-empty, replaces the
  `config:` map entirely (still validated by the app at startup). Use this to
  supply a config you already manage elsewhere:

  ```bash
  helm install sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
    --version 0.2.0 -n sf2loki --create-namespace \
    --set-file configOverride=./config.yaml \
    -f my-values.yaml
  ```

`rolloutOnConfigChange` (default `true`) adds a checksum pod annotation so a
config change rolls the pods — config is read only at startup.

## Secrets

sf2loki reads secrets from **files**, not environment variables, mounted at
`/etc/sf2loki/secrets`. Filenames must match whatever `*_file` paths your
config references (e.g. `server.key`, `loki-token`, `client-secret`). Three
ways to get them there:

1. **Chart-managed Secret** (`secrets.create: true`) — pass contents via
   `--set-file` rather than committing plaintext into a values file:

   ```bash
   helm install sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
     --version 0.2.0 -n sf2loki --create-namespace \
     --set secrets.create=true \
     --set-file secrets.data.server\.key=./server.key \
     --set-file secrets.data.loki-token=./loki-token \
     -f my-values.yaml
   ```

2. **Pre-created Secret** (`secrets.existingSecret: <name>`) — you own the
   `Secret` object (e.g. synced by a GitOps controller); the chart only
   references it by name.

3. **External Secrets Operator** (`externalSecrets.enabled: true`) — the
   chart renders a `SecretStore` + `ExternalSecret` that build the mounted
   Secret from AWS Secrets Manager or GCP Secret Manager
   (`externalSecrets.provider: aws | gcp`), keyed by `externalSecrets.data`
   entries mapping a mount filename to a `remoteRef`. Requires ESO already
   installed in the cluster, and (for cloud auth) a separate
   `serviceAccount.esoName` provisioned out-of-band with the right IAM/
   workload-identity binding.

## High availability

sf2loki is **single-instance by default**. The Salesforce Pub/Sub API has no
consumer-group semantics, so two replicas both subscribing **double-deliver**
every event — the chart enforces this: `replicaCount > 1` without
`ha.enabled: true` **fails the render**.

The full active-passive recipe needs three things to agree:

```yaml
replicaCount: 2

ha:
  enabled: true
  leaseName: sf2loki-leader   # must equal config.coordinate.k8s_lease.name

config:
  coordinate:
    type: k8s_lease
    k8s_lease:
      namespace: ${POD_NAMESPACE}   # injected via the downward API — leave as-is
      name: sf2loki-leader          # must equal ha.leaseName
  state:
    store: s3   # or gcs — file is a per-pod emptyDir, INVALID for HA
    s3:
      bucket: my-sf2loki-state-bucket
      key: sf2loki/state.json
```

`ha.enabled: true` additionally renders:

- **Lease RBAC** — a namespace-scoped `Role`/`RoleBinding` granting
  `get`/`create`/`update` on `coordination.k8s.io` `leases`, nothing
  cluster-wide.
- **Anti-affinity + topology spread** — best-effort host and zone spread so a
  single node/AZ failure can't take out both replicas.

See [High Availability](high-availability.md) for the coordinator model and
[State & Checkpoints](state.md) for the S3/GCS backend detail.

## Hardening & NetworkPolicy

The container runs as uid/gid `10001`, `readOnlyRootFilesystem: true`, all
capabilities dropped, `seccompProfile: RuntimeDefault` — none of this is
configurable away, it ships hardened by default.

`networkPolicy.enabled` (default `false`) renders a default-deny
ingress+egress policy that permits DNS, the health port, and the sf2loki
egress set (Salesforce REST/SOAP, Pub/Sub gRPC, Loki, OTLP, the state store,
and the Kubernetes API when `ha.enabled`).

!!! warning "Plain NetworkPolicy can't match Salesforce/Pub/Sub by hostname"
    Standard Kubernetes `NetworkPolicy` is CIDR/label-based, not FQDN-based —
    it cannot match `*.my.salesforce.com` or `api.pubsub.salesforce.com`. The
    chart's `networkPolicy.*CIDR` values default to `0.0.0.0/0` (permissive
    placeholders); tighten them to your provider's published ranges, or use a
    CNI with FQDN-aware policy (Cilium `toFQDNs` — see
    `templates/networkpolicy.yaml` for where to add it). Note Pub/Sub egress
    is gRPC on **port 7443**, not 443. On EKS, enforcement also needs the VPC
    CNI network-policy feature (or Calico) enabled, or these rules are a no-op.

## Upgrades & uninstall

```bash
helm upgrade sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
  --version <new-version> -n sf2loki -f my-values.yaml
```

A config change rolls the pods via the checksum annotation
(`rolloutOnConfigChange`); `rollingUpdate.maxUnavailable: 1` keeps a healthy
standby available to take the Lease during an HA rollout.

```bash
helm uninstall sf2loki -n sf2loki
```

There is no cleanup hook: the app-created `Lease` simply expires on its own
(harmless), and a shared S3/GCS state store **persists on purpose** — delete
it manually if you want to reset the watermark and re-ingest from scratch.

## The two sharp edges

!!! danger "Never point `livenessProbe` at `/readyz`"
    The standby replica in an HA pair returns `503 standby` on `/readyz`
    **forever, by design** — it isn't becoming ready, it's supposed to stand
    by. That's exactly what a `readinessProbe` should see: it keeps traffic
    off the standby. Pointing `livenessProbe` at `/readyz` instead
    restart-loops the standby forever and defeats failover entirely. The
    chart wires this correctly via `probes.readiness` (→ `/readyz`) and
    `probes.liveness` (→ `/healthz`) — don't override one with the other.

!!! danger "`terminationGracePeriodSeconds` must cover `service.shutdown_grace` plus the app's own close budget"
    `terminationGracePeriodSeconds` defaults to `40` (25s default
    `config.service.shutdown_grace` + ~5s closer budget + margin). If you
    raise `shutdown_grace` in `config`, raise `terminationGracePeriodSeconds`
    to match, or kubelet SIGKILLs the pod mid-drain (final flush, checkpoint
    commit, store close) on every rollout or eviction.

## See also

- [High Availability](high-availability.md) — the coordinator model `ha.enabled` wires up.
- [State & Checkpoints](state.md) — the S3/GCS backend the HA state store requires.
- [Configuration reference](../config-reference.md) — `CoordinateConfig` /
  `K8sLeaseConfig` field details.
- [Chart README](https://github.com/rknightion/sf2loki/tree/main/deploy/helm) —
  the full `values.yaml` with every knob and its default.
