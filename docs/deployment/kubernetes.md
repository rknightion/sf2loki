# Kubernetes

[`deploy/k8s/`](https://github.com/rknightion/sf2loki/tree/main/deploy/k8s) has example
manifests for running sf2loki's active-passive HA pair with the `k8s_lease` coordinator
(`coordinate.type: k8s_lease`). They are a starting point to adapt, **not** a Helm chart
or Kustomize base — see [Deployment](index.md#known-gaps) for that gap. Namespaces,
image tag, resource sizing, and the config/secret contents are all yours to fill in.

| File | What it is |
| --- | --- |
| [`serviceaccount.yaml`](https://github.com/rknightion/sf2loki/blob/main/deploy/k8s/serviceaccount.yaml) | The ServiceAccount the pod runs as. |
| [`rbac.yaml`](https://github.com/rknightion/sf2loki/blob/main/deploy/k8s/rbac.yaml) | `Role` + `RoleBinding` granting `get`/`create`/`update` on `leases` in the namespace — exactly what the coordinator needs, nothing cluster-wide. |
| [`deployment.yaml`](https://github.com/rknightion/sf2loki/blob/main/deploy/k8s/deployment.yaml) | The 2-replica Deployment: probes, `terminationGracePeriodSeconds`, resource requests/limits, config/secret volume mounts. |
| [`service.yaml`](https://github.com/rknightion/sf2loki/blob/main/deploy/k8s/service.yaml) | Optional `ClusterIP` Service that routes only to the current leader (the Ready pod). |

## Before you apply these

1. **Namespace.** All manifests use `sf2loki`; create it
   (`kubectl create namespace sf2loki`) or adapt them to your own.
2. **Config.** Create a `ConfigMap` named `sf2loki-config` with a `config.yaml` key —
   the same schema as `config.docker.yaml`, but with `coordinate.type: k8s_lease` plus a
   `coordinate.k8s_lease` block (see [High Availability](high-availability.md)), and
   `state.store: s3` or `gcs` — the HA state store must be shared between replicas; the
   local `file` store is per-pod and **not** valid for this topology:
   ```bash
   kubectl -n sf2loki create configmap sf2loki-config --from-file=config.yaml=./config.k8s.yaml
   ```
3. **Secrets.** Create a `Secret` named `sf2loki-secrets` with the private key / Loki
   token / any other `*_file` values your config references, mirroring the `./secrets`
   mount in `docker-compose.yml`:
   ```bash
   kubectl -n sf2loki create secret generic sf2loki-secrets \
     --from-file=server.key=./secrets/server.key \
     --from-file=loki-token=./secrets/loki-token
   ```
4. **Non-secret env (optional).** If your config uses `${VAR}` interpolation, create a
   ConfigMap named `sf2loki-env`
   (`kubectl -n sf2loki create configmap sf2loki-env --from-env-file=.env.dev`) —
   `deployment.yaml` references it as `optional: true`, so its absence isn't fatal if
   you'd rather bake everything into `config.yaml` directly.
5. **Image tag.** `deployment.yaml` defaults to `:latest` (the same released-tag default
   as `docker-compose.yml`); pin `:main-<sha>` if you need a specific edge build.

Then:

```bash
kubectl apply -f serviceaccount.yaml -f rbac.yaml -f deployment.yaml -f service.yaml
```

## The two sharp edges

!!! danger "Never point `livenessProbe` at `/readyz`"
    The standby replica in an HA pair returns `503 standby` on `/readyz` **forever, by
    design** — it isn't becoming ready, it's supposed to stand by. That's exactly what a
    `readinessProbe` should see: it keeps traffic off the standby. Pointing
    `livenessProbe` at `/readyz` instead restart-loops the standby forever and defeats
    failover entirely.

    ```yaml
    readinessProbe:
      httpGet: { path: /readyz, port: health }
    livenessProbe:
      httpGet: { path: /healthz, port: health }
    ```

- **`terminationGracePeriodSeconds` must cover `service.shutdown_grace` plus the app's
  own close budget.** `deployment.yaml` uses 40s (25s default `shutdown_grace` + ~5s
  closer budget + margin). If you raise `shutdown_grace` in your config, raise this to
  match, or kubelet SIGKILLs the pod mid-drain (final flush, checkpoint commit, store
  close) on every rollout or eviction.

## See also

- [High Availability](high-availability.md) — the coordinator model these manifests
  wire up.
- [State & Checkpoints](state.md) — the S3/GCS backend the HA state store requires.
- [Configuration reference](../config-reference.md) — `CoordinateConfig` /
  `K8sLeaseConfig` field details.
