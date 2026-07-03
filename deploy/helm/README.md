# sf2loki Helm chart

Deploys the sf2loki service: a Deployment running the container, a ConfigMap holding
`config.yaml` (+ an optional `-env` ConfigMap for `${VAR}` interpolation), a Secret for the
mounted credential files, a ServiceAccount, an optional ClusterIP Service and
PodDisruptionBudget, and — only under HA — the RBAC (`Role`/`RoleBinding`) the Kubernetes
Lease coordinator needs. NetworkPolicy and External Secrets Operator wiring are opt-in.

See [`docs/deployment/kubernetes.md`](https://m7kni.io/sf2loki/deployment/kubernetes/) for the
full walkthrough and the main [README](../../README.md) for what sf2loki does. This chart
replaces the example manifests that used to live in `deploy/k8s/` — read `values.yaml` itself
too, its comments are the primary reference for every knob.

## Prerequisites

- Kubernetes >= 1.21 (the floor is `PodDisruptionBudget policy/v1`, stable since 1.21).
- Helm 3+ (published as an OCI chart; no `helm repo add` needed).
- The image is already published — you don't build or push anything to install this chart.
- A Salesforce Connected App / External Client App and a Loki push endpoint. The `config:`
  block below is a **template**, not a working config — see [Config](#config) before you
  install for real.

## Quick install (single instance)

sf2loki ships as an OCI chart at `oci://ghcr.io/rknightion/charts/sf2loki`, published by the
shared container-publish pipeline on every release.

```bash
helm install sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
  --version <x.y.z> \
  -n sf2loki --create-namespace \
  -f my-values.yaml
```

A minimal `my-values.yaml` for a single instance, `client_credentials` auth, and secrets
supplied out-of-band:

```yaml
secrets:
  existingSecret: sf2loki-secrets  # keys: client-secret, loki-token

config:
  salesforce:
    login_url: https://my-domain.my.salesforce.com
    auth_mode: client_credentials
    client_id: ${SF_CLIENT_ID}
  sources:
    pubsub:
      topics: [/event/LoginEventStream]
  sink:
    loki:
      url: https://logs-prod-XXX.grafana.net/loki/api/v1/push
      tenant_id: "123456"

env:
  SF_CLIENT_ID: "3MVG9..."
```

Check rollout and logs the same way you would for any Deployment:

```bash
kubectl -n sf2loki rollout status deploy/sf2loki
kubectl -n sf2loki logs deploy/sf2loki
```

## Config

The `config:` map in `values.yaml` is **generated from the Pydantic schema** (`just
gen-helm-values` on the sf2loki repo) and rendered verbatim into the `sf2loki-config`
ConfigMap. It documents every field with the model's own descriptions and defaults, but it is
a template: pick `config.salesforce` **or** `config.orgs` (exactly one — the generated file
shows both for reference) and fill in the empty required values (`login_url`, `client_id`,
`username`, the Loki `url`, …). Set values under your own `my-values.yaml`, `--set
config.salesforce.login_url=…`, or replace the whole block with `configOverride` (a raw
`config.yaml` string, e.g. `--set-file configOverride=./config.yaml`) if you'd rather manage
the file yourself. `rolloutOnConfigChange` (default on) restarts pods on a config change since
sf2loki only reads config at startup.

## High availability

sf2loki is **single-instance by default**. The Salesforce Pub/Sub API has no consumer-group
semantics, so two replicas both subscribing double-deliver every event — `replicaCount>1`
without `ha.enabled=true` **fails the chart render** rather than silently double-ingesting.

HA is active-passive via a Kubernetes `Lease`: one replica holds the lease and runs, the rest
stand by (and report `503` on `/readyz` by design — that's what keeps the Service off them).
To turn it on you need three things to agree:

```yaml
replicaCount: 2

ha:
  enabled: true
  leaseName: sf2loki-leader   # must equal config.coordinate.k8s_lease.name

config:
  coordinate:
    type: k8s_lease
    k8s_lease:
      namespace: ${POD_NAMESPACE}   # injected via the downward API
      name: sf2loki-leader

  state:
    store: s3   # or gcs — a SHARED store; the local `file` store is per-pod and INVALID for HA
    s3:
      bucket: my-sf2loki-state
```

`ha.enabled: true` also makes the chart render the lease RBAC (`Role`/`RoleBinding` scoped to
exactly `ha.leaseName`, nothing cluster-wide) and add best-effort pod anti-affinity plus
host/zone topology spread, so the pair doesn't land on the same node or AZ. A 3rd replica only
adds an idle standby — HA is 2 replicas, not N.

The published image already carries the `sf2loki[k8s]` extra needed for `coordinate.type:
k8s_lease`; a shared `state.store: s3` or `gcs` additionally needs the `sf2loki[s3]`/`[gcs]`
extra (also already in the image).

## Secrets

sf2loki reads credentials from **mounted files** at `/etc/sf2loki/secrets`, never environment
variables — each key in the Secret becomes a filename, and the `*_file` paths in `config` must
match (e.g. `server.key` -> `/etc/sf2loki/secrets/server.key`). Pick exactly one of:

1. **`secrets.create: true` + `secrets.data`** — the chart creates the Secret from the values
   you give it. Committing plaintext into a values file is discouraged (it lands in `helm get
   values` / release history / git in cleartext); prefer keeping the file out of the values
   file entirely:
   ```bash
   helm upgrade --install sf2loki oci://ghcr.io/rknightion/charts/sf2loki \
     --set secrets.create=true \
     --set-file secrets.data.server\.key=./server.key \
     --set-file secrets.data.loki-token=./loki-token.txt \
     -f my-values.yaml
   ```
2. **`secrets.existingSecret: <name>`** — reference a Secret you created (or another tool
   manages) out-of-band. This is the default assumption in the quick-install example above.
3. **`externalSecrets.enabled: true`** — see [External Secrets](#external-secrets) below; the
   chart builds the Secret for you from a cloud secrets manager.

## NetworkPolicy

Opt-in (`networkPolicy.enabled`, default off) — most clusters already run a default-deny
baseline or a service mesh, and getting egress CIDRs wrong here fails silently (a blocked
Salesforce Pub/Sub connection just looks like a hung stream). When enabled it default-denies
ingress+egress, then permits: the health port (kubelet probes, from anywhere — there's no
portable selector for "the node"), DNS via a namespace+pod selector, Salesforce REST/SOAP
(443), Salesforce Pub/Sub (7443, a **distinct** port from REST), Loki push (443), OTLP metrics
(443, reuses the Loki CIDR when `otlpCIDR` is empty), the shared state store (443, only
relevant with `state.store: s3|gcs`), and — only under `ha.enabled` — the Kubernetes API
server for Lease leader election.

Every `*CIDR` value defaults to `0.0.0.0/0` — a **placeholder**, not a real restriction.
Standard Kubernetes `NetworkPolicy` is CIDR/label-based and cannot match FQDNs like
`*.my.salesforce.com` (per-org, DNS-resolved, not a stable IP). Either tighten each CIDR to
your provider's published ranges, or use a CNI with FQDN-aware policy — Cilium's
`CiliumNetworkPolicy` (`toFQDNs`) is the template's own suggested escape hatch; see the
comments in `templates/networkpolicy.yaml` for a worked example. On EKS, enforcement also
needs the VPC CNI network-policy feature (or Calico) turned on, or these rules are a no-op.

## External Secrets

Opt-in (`externalSecrets.enabled`, default off) — an alternative to `secrets.create` /
`secrets.existingSecret` that builds the same mounted-file Secret from a cloud secrets manager
via the [External Secrets Operator](https://external-secrets.io/). Requires ESO already
installed in the cluster and a workload-identity-annotated ServiceAccount provisioned
out-of-band (`serviceAccount.esoName`) with read access to the manager. Pick a provider and
map each mounted filename to a remote key:

```yaml
serviceAccount:
  esoName: sf2loki-eso   # IRSA/workload-identity SA, provisioned by your infra IaC

externalSecrets:
  enabled: true
  provider: aws            # or gcp
  aws:
    region: eu-west-1
  refreshInterval: 1h
  data:
    - secretKey: server.key
      remoteRef:
        key: sf2loki/server-key
    - secretKey: loki-token
      remoteRef:
        key: sf2loki/loki-token
```

## Uninstall

```bash
helm uninstall sf2loki -n sf2loki
```

Removes every resource this chart created. There is no cleanup hook: the Kubernetes `Lease`
(when `ha.enabled`) is created by the app itself, not the chart, and simply **expires** after
the pods are gone — harmless. A shared S3/GCS state store (`config.state.store: s3|gcs`)
**persists on purpose** — it's not chart-owned either — so a reinstall resumes from the same
watermark. Delete the state object yourself if you actually want to reset it.
