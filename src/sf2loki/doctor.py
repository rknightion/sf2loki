"""`sf2loki doctor`: live end-to-end preflight diagnostics.

Runs a sequenced, read-only-except-one-test-write check of the whole path —
config, auth, Salesforce permissions/entitlements, Pub/Sub reachability, the
Loki write path, and the state directory — and prints a PASS/WARN/FAIL table
so first-run problems surface in one command instead of one at a time at
runtime.

Sequencing: each check after ``config`` assumes the ones before it succeeded
(a check that needs an access token is SKIPped when ``auth`` failed, etc.) —
see :func:`run_doctor` for the exact dependency graph.
"""

from __future__ import annotations

import contextlib
import fcntl
import fnmatch
import importlib.util
import json
import os
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import httpx

from sf2loki.app import App
from sf2loki.auth.jwt_auth import AccessToken, AuthError, TokenProvider
from sf2loki.config import (
    EVENT_TYPE_WILDCARD,
    Config,
    ConfigError,
    FileLeaseConfig,
    FileStateConfig,
    K8sLeaseConfig,
    PubSubConfig,
    SalesforceConfig,
    StateConfig,
    TransformRule,
    as_single_org_view,
    load,
    select_org,
    telemetry_headers,
)
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.apexlog_client import ApexLogClient, ApexLogError
from sf2loki.salesforce.eventlogfile_client import EventLogFileClient, EventLogFileError
from sf2loki.salesforce.limits_client import LimitsClient, LimitsError
from sf2loki.salesforce.pubsub_client import PubSubClient
from sf2loki.salesforce.soql_client import SoqlClient, SoqlError
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink
from sf2loki.state import build_store
from sf2loki.transforms import unsalted_hash_warnings

# Explicit HTTP timeouts for the shared clients this command owns — mirrors
# App.build's _HTTP_TIMEOUT (httpx's 5s-everywhere default churns on slow
# Salesforce responses / large-ish Loki test pushes). Kept as an independent
# constant rather than importing app.py's private one so this file has no
# coupling to App's internals beyond the App.build() call itself.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)

# Probe file name for the "state" check (file backend) — deliberately NOT the
# real state file name: the running daemon may hold an flock on that one.
_STATE_PROBE_NAME = ".sf2loki-doctor-probe"

# Probe object suffix for the "state" check (s3/gcs backends) — appended to
# the real key/object name so the probe lives at a doctor-namespaced
# location in the SAME bucket, never the real checkpoint object.
_STATE_OBJECT_PROBE_SUFFIX = ".sf2loki-doctor-probe"
_STATE_OBJECT_PROBE_KEY = "doctor"

# Probe file name for the "coordinator" check (file_lease backend) —
# deliberately NOT the real lease file: a live leader may be renewing it.
_COORDINATOR_LEASE_PROBE_NAME = ".sf2loki-doctor-coordinator-probe"

# WARN threshold for the "limits" check: DailyApiRequests remaining/max ratio.
_LOW_LIMIT_RATIO = 0.2

CheckStatus = Literal["PASS", "WARN", "FAIL", "SKIP"]


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One row of the doctor table."""

    name: str
    status: CheckStatus
    detail: str


# ---------------------------------------------------------------------------
# Check 1: config
# ---------------------------------------------------------------------------


async def _check_config(config_path: Path | None) -> tuple[Config | None, CheckResult]:
    """Load the config and validate wiring via ``App.build`` (no network calls).

    Returns ``(None, FAIL result)`` on any problem — callers must then SKIP
    every remaining check (they all need a valid ``Config``).
    """
    try:
        cfg = load(config_path)
    except ConfigError as exc:
        return None, CheckResult("config", "FAIL", str(exc))

    try:
        app = App.build(cfg)
    except Exception as exc:
        return None, CheckResult("config", "FAIL", str(exc))
    # App.build() only constructs objects (httpx clients, a lazily-connected
    # PubSubClient, ...) to validate wiring — close what it opened immediately
    # since this App instance is never run.
    await app._shutdown()
    return cfg, CheckResult("config", "PASS", "configuration and wiring valid")


# ---------------------------------------------------------------------------
# Check 2: auth
# ---------------------------------------------------------------------------


def _auth_failure_hint(auth_mode: str, login_url: str, message: str) -> str:
    """Best-effort likely-cause hint appended to an auth FAIL detail."""
    if auth_mode == "client_credentials" and login_url in (
        "https://login.salesforce.com",
        "https://test.salesforce.com",
    ):
        return (
            "client_credentials requires the org's My Domain token endpoint; set "
            "salesforce.login_url to e.g. https://yourorg.my.salesforce.com"
        )
    if auth_mode == "jwt_bearer" and "invalid_grant" in message:
        return (
            "invalid_grant usually means the integration user is not pre-authorised "
            "on the External Client App's Policies tab, or the certificate does not "
            "match the private key"
        )
    return ""


async def _check_auth(
    sf: SalesforceConfig, tokens: TokenProvider
) -> tuple[AccessToken | None, str | None, CheckResult]:
    """Mint a token and resolve the org id; report the flow, instance, and org."""
    try:
        tok = await tokens.token()
        org_id = sf.org_id or await tokens.org_id()
    except AuthError as exc:
        hint = _auth_failure_hint(sf.auth_mode, sf.login_url, str(exc))
        detail = f"{exc} — {hint}" if hint else str(exc)
        return None, None, CheckResult("auth", "FAIL", detail)

    detail = f"flow={sf.auth_mode} instance_url={tok.instance_url} org_id={org_id}"
    return tok, org_id, CheckResult("auth", "PASS", detail)


# ---------------------------------------------------------------------------
# Check 3: permissions
# ---------------------------------------------------------------------------


async def _check_permissions(
    cfg: Config,
    sf: SalesforceConfig,
    tok: AccessToken,
    http_client: httpx.AsyncClient,
    tokens: TokenProvider,
    metrics: Metrics,
) -> CheckResult:
    """Prove 'View Event Log Files' via describe; optionally probe one stored object."""
    url = f"{tok.instance_url}/services/data/v{sf.api_version}/sobjects/EventLogFile/describe"
    try:
        resp = await http_client.get(url, headers={"Authorization": f"Bearer {tok.value}"})
    except httpx.HTTPError as exc:
        return CheckResult("permissions", "FAIL", f"EventLogFile describe request failed: {exc}")

    if resp.status_code == 403:
        return CheckResult(
            "permissions",
            "FAIL",
            "HTTP 403 describing EventLogFile — the integration user likely lacks the "
            f"'View Event Log Files' permission: {resp.text}",
        )
    if not resp.is_success:
        return CheckResult(
            "permissions",
            "FAIL",
            f"EventLogFile describe failed: HTTP {resp.status_code} — {resp.text}",
        )

    detail = "EventLogFile describe OK (View Event Log Files granted)"
    eventlog_objects = cfg.sources.eventlog_objects
    if eventlog_objects.enabled and eventlog_objects.objects:
        obj_name = eventlog_objects.objects[0].name
        soql = SoqlClient(sf, tokens, http_client, metrics=metrics)
        try:
            async for _ in soql.query(f"SELECT Id FROM {obj_name} LIMIT 1"):
                pass
        except SoqlError as exc:
            return CheckResult("permissions", "FAIL", f"SOQL probe of {obj_name} failed: {exc}")
        detail += f"; SOQL probe of {obj_name} OK"
    return CheckResult("permissions", "PASS", detail)


# ---------------------------------------------------------------------------
# Check 4: pubsub
# ---------------------------------------------------------------------------


def _resolve_pubsub_topics(cfg: PubSubConfig) -> list[str]:
    """Explicitly-configured topics after de-dupe + include/exclude filtering.

    Mirrors ``PubSubSource._filter`` (a private method on a class this check
    has no other reason to construct): the ``"*"`` discovery marker is never a
    literal topic, and discovered topics are resolved only at runtime, so they
    are deliberately not probed here.
    """
    seen: set[str] = set()
    result: list[str] = []
    for topic in cfg.topics:
        if topic == "*" or topic in seen:
            continue
        seen.add(topic)
        if not any(fnmatch.fnmatchcase(topic, pat) for pat in cfg.include):
            continue
        if any(fnmatch.fnmatchcase(topic, pat) for pat in cfg.exclude):
            continue
        result.append(topic)
    return result


def _format_topic_error(exc: Exception) -> str:
    """Render a gRPC error's code+details when available, else str(exc)."""
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    if callable(code) and callable(details):
        return f"{code()}: {details()}"
    return str(exc)


async def _check_pubsub(cfg: Config, tokens: TokenProvider, metrics: Metrics) -> list[CheckResult]:
    """Probe each resolved explicit topic via GetTopic; one row per topic.

    Wildcard discovery is never run here (that happens at pipeline runtime) —
    a ``topics: ["*"]`` config gets a single WARN row instead.
    """
    ps_cfg = cfg.sources.pubsub
    if not ps_cfg.enabled:
        return [CheckResult("pubsub", "SKIP", "pubsub source disabled")]
    if not ps_cfg.topics:
        return [CheckResult("pubsub", "SKIP", "no topics configured")]
    if ps_cfg.topics == [EVENT_TYPE_WILDCARD]:
        return [
            CheckResult(
                "pubsub",
                "WARN",
                "wildcard — topics resolved at runtime, per-topic reachability not probed",
            )
        ]

    topics = _resolve_pubsub_topics(ps_cfg)
    if not topics:
        return [CheckResult("pubsub", "WARN", "no topics survived the include/exclude filters")]

    client = PubSubClient(ps_cfg, tokens, metrics=metrics)
    results: list[CheckResult] = []
    try:
        for topic in topics:
            try:
                await client.get_topic(topic)
            except Exception as exc:
                results.append(CheckResult(f"pubsub:{topic}", "FAIL", _format_topic_error(exc)))
            else:
                results.append(CheckResult(f"pubsub:{topic}", "PASS", "topic reachable"))
    finally:
        await client.aclose()
    return results


# ---------------------------------------------------------------------------
# Check 5: entitlement
# ---------------------------------------------------------------------------


async def _check_entitlement(
    cfg: Config,
    sf: SalesforceConfig,
    tokens: TokenProvider,
    http_client: httpx.AsyncClient,
    metrics: Metrics,
) -> CheckResult:
    """Report configured explicit EventTypes that have produced no files."""
    elf_cfg = cfg.sources.eventlogfile
    if not elf_cfg.enabled:
        return CheckResult("entitlement", "SKIP", "eventlogfile source disabled")

    client = EventLogFileClient(sf, tokens, http_client, metrics=metrics)
    try:
        produced = await client.list_event_types(elf_cfg.interval)
    except EventLogFileError as exc:
        return CheckResult("entitlement", "FAIL", f"EventType discovery failed: {exc}")

    explicit = [t.name for t in elf_cfg.event_types if t.name != EVENT_TYPE_WILDCARD]
    missing = [name for name in explicit if name not in produced]
    detail = f"org produces {len(produced)} {elf_cfg.interval} EventType(s) total"
    if missing:
        detail += (
            f"; no {elf_cfg.interval} files found for {', '.join(missing)} — check Event "
            "Monitoring entitlement or the type name"
        )
        return CheckResult("entitlement", "WARN", detail)
    return CheckResult("entitlement", "PASS", detail)


# ---------------------------------------------------------------------------
# Check 5b: traceflags (apexlog prerequisite)
# ---------------------------------------------------------------------------


async def _check_traceflags(
    cfg: Config,
    sf: SalesforceConfig,
    tokens: TokenProvider,
    http_client: httpx.AsyncClient,
    metrics: Metrics,
) -> CheckResult:
    """Warn when apexlog is enabled but no active TraceFlags exist (no logs will flow)."""
    if not cfg.sources.apexlog.enabled:
        return CheckResult("traceflags", "SKIP", "apexlog source disabled")
    client = ApexLogClient(sf, tokens, http_client, metrics=metrics)
    try:
        active = await client.count_active_traceflags()
    except ApexLogError as exc:
        return CheckResult("traceflags", "WARN", f"could not query TraceFlags: {exc}")
    if active == 0:
        return CheckResult(
            "traceflags",
            "WARN",
            "apexlog enabled but no active TraceFlags — no ApexLog rows will be generated. "
            "Enable debug logging for the target user(s) via `sf debug` or Setup -> Debug Logs.",
        )
    return CheckResult("traceflags", "PASS", f"{active} active TraceFlag(s)")


# ---------------------------------------------------------------------------
# Check 6: loki
# ---------------------------------------------------------------------------


async def _check_loki(cfg: Config) -> CheckResult:
    """Push exactly one test line to the sink — the only write this command performs."""
    loki_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    try:
        sink = LokiSink(cfg.sink.loki, loki_http, metrics=Metrics())
        now = datetime.now(UTC)
        entry = LogEntry(
            timestamp=now,
            labels={"source": "sf2loki-doctor"},
            line=json.dumps({"msg": "sf2loki doctor test write", "ts": now.isoformat()}),
            structured_metadata={},
            checkpoint=CheckpointToken(key="doctor", value="doctor"),
        )
        batch = Batch(entries=[entry])
        t0 = time.monotonic()
        try:
            await sink.push(batch)
        except RetryableSinkError as exc:
            hint = ""
            if "401" in str(exc) or "403" in str(exc):
                hint = (
                    " — check that sink.loki.tenant_id (the numeric Loki 'User' id from "
                    "Cloud Portal → your stack → Loki → Details) matches the stack the "
                    "auth_token was issued for, and that the token has the logs:write scope"
                )
            return CheckResult("loki", "FAIL", f"{exc}{hint}")
        except PermanentSinkError as exc:
            return CheckResult("loki", "FAIL", str(exc))
        latency_ms = (time.monotonic() - t0) * 1000
        return CheckResult(
            "loki",
            "PASS",
            f"pushed 1 test line in {latency_ms:.0f}ms (the only write this command performs)",
        )
    finally:
        await loki_http.aclose()


# ---------------------------------------------------------------------------
# Check 6b: transforms (issue #67)
# ---------------------------------------------------------------------------


def _check_transforms(cfg: Config) -> CheckResult:
    """Warn when any configured ``hash`` transform rule would run unsalted.

    Gathers every source's ``transforms`` list (each source config carries
    its own) and checks them against the deployment-wide
    ``sources.transform_salt`` via :func:`sf2loki.transforms.unsalted_hash_warnings`
    — the same helper ``app.py``'s startup/``--check`` path calls.
    """
    salt = cfg.sources.transform_salt.get_secret_value() if cfg.sources.transform_salt else ""
    rules: list[TransformRule] = [
        *cfg.sources.pubsub.transforms,
        *cfg.sources.eventlog_objects.transforms,
        *cfg.sources.eventlogfile.transforms,
        *cfg.sources.apexlog.transforms,
    ]
    warnings = unsalted_hash_warnings(rules, salt)
    if warnings:
        return CheckResult("transforms", "WARN", "; ".join(warnings))
    if not any(rule.action == "hash" for rule in rules):
        return CheckResult("transforms", "SKIP", "no hash transform rules configured")
    return CheckResult("transforms", "PASS", "all configured hash transform rules are salted")


# ---------------------------------------------------------------------------
# Check 6c: telemetry
# ---------------------------------------------------------------------------


async def _check_telemetry(cfg: Config) -> CheckResult:
    """No-op OTLP/HTTP metrics POST when telemetry is enabled.

    An empty ``ExportMetricsServiceRequest`` protobuf message serializes to
    zero bytes, so POSTing an empty body with the OTLP content-type exercises
    endpoint reachability and auth — without emitting any real metric data or
    needing the protobuf/OTel SDK on this path. Mirrors the Loki test-write
    check's shape; the "write" here is a valid empty export every OTLP/HTTP
    receiver accepts as a no-op.
    """
    telemetry = cfg.service.telemetry
    if not telemetry.enabled:
        return CheckResult("telemetry", "SKIP", "service.telemetry.enabled is false")
    if not telemetry.endpoint:
        return CheckResult(
            "telemetry", "FAIL", "service.telemetry.enabled is true but endpoint is empty"
        )

    headers = telemetry_headers(telemetry, cfg.sink.loki)
    headers.setdefault("Content-Type", "application/x-protobuf")
    otlp_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    try:
        try:
            resp = await otlp_http.post(telemetry.endpoint, content=b"", headers=headers)
        except httpx.HTTPError as exc:
            return CheckResult(
                "telemetry", "FAIL", f"OTLP endpoint {telemetry.endpoint} unreachable: {exc}"
            )
    finally:
        await otlp_http.aclose()

    if resp.status_code in (401, 403):
        return CheckResult(
            "telemetry",
            "FAIL",
            f"OTLP endpoint {telemetry.endpoint} returned HTTP {resp.status_code} — check "
            "service.telemetry.basic_auth_user/basic_auth_token (defaults to the Loki "
            "sink's tenant_id/auth_token — a common mistake is the OTLP instance id vs "
            f"the Loki tenant id): {resp.text}",
        )
    if not resp.is_success:
        return CheckResult(
            "telemetry",
            "FAIL",
            f"OTLP endpoint {telemetry.endpoint} rejected an empty test export: "
            f"HTTP {resp.status_code} — {resp.text}",
        )
    return CheckResult(
        "telemetry", "PASS", f"OTLP endpoint {telemetry.endpoint} reachable and authorized"
    )


# ---------------------------------------------------------------------------
# Check 7: state
# ---------------------------------------------------------------------------


async def _check_state(cfg: Config) -> CheckResult:
    """Dispatch on ``state.store``: probe whichever backend is actually configured.

    Fixes the "doctor validates a state dir the deployment doesn't use" gap
    (issue #59) — the file-backend check ran unconditionally even for s3/gcs
    deployments, which never touch that local directory.
    """
    if cfg.state.store == "file":
        return _check_state_file(cfg.state.file)
    return await _check_state_object(cfg.state)


def _check_state_file(file_cfg: FileStateConfig) -> CheckResult:
    """Create+lock+delete a probe file in the state directory (never the real state file)."""
    state_dir = file_cfg.path.parent
    probe_path = state_dir / _STATE_PROBE_NAME
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(probe_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.write(fd, b"sf2loki doctor probe")
            os.fsync(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
    except PermissionError as exc:
        return CheckResult(
            "state",
            "FAIL",
            f"cannot write to state directory {state_dir}: permission denied ({exc}). The "
            "service runs as uid 10001 in the container, so the state directory must be "
            "writable by it — e.g. `chmod 770` + `chown` it to a group the container user "
            "is in, or mount it group-writable (a root-owned 0700 directory crash-loops "
            "the container).",
        )
    except OSError as exc:
        return CheckResult("state", "FAIL", f"state directory {state_dir} is not usable: {exc}")
    finally:
        # Best-effort cleanup only: on the PermissionError path above the
        # directory may deny unlink too — never let cleanup mask the FAIL
        # result already produced (or crash a PASS result) by re-raising.
        with contextlib.suppress(OSError):
            probe_path.unlink(missing_ok=True)
    return CheckResult("state", "PASS", f"state directory {state_dir} is writable and lockable")


def _probe_state_config(state: StateConfig) -> StateConfig:
    """A copy of *state* pointed at a doctor-namespaced probe key/object.

    Same bucket/credentials as *state*, but a different key (s3) / object
    name (gcs) — so probing an s3/gcs backend never reads or writes the real
    checkpoint document (mirrors the file backend's separate probe filename).
    """
    if state.store == "s3":
        probe_key = f"{state.s3.key}{_STATE_OBJECT_PROBE_SUFFIX}"
        probe_s3 = state.s3.model_copy(update={"key": probe_key})
        return state.model_copy(update={"s3": probe_s3})
    if state.store == "gcs":
        probe_gcs = state.gcs.model_copy(
            update={"object_name": f"{state.gcs.object_name}{_STATE_OBJECT_PROBE_SUFFIX}"}
        )
        return state.model_copy(update={"gcs": probe_gcs})
    return state


def _state_object_target(probe: StateConfig) -> str:
    if probe.store == "s3":
        return f"s3://{probe.s3.bucket}/{probe.s3.key}"
    return f"gs://{probe.gcs.bucket}/{probe.gcs.object_name}"


async def _check_state_object(state: StateConfig) -> CheckResult:
    """s3/gcs: round-trip load+commit through a doctor-namespaced probe object.

    Proves auth, bucket/object reachability, and write permission against the
    ACTUAL configured backend — never the real checkpoint key. Reuses
    ``build_store`` (including its friendly missing-extra error) pointed at a
    probe-suffixed config via :func:`_probe_state_config`. The
    ``CheckpointStore`` seam has no delete, so the probe object persists
    across runs — cheap (a few bytes) and harmless since it never shares a
    key with real checkpoint data; ``commit`` reuses/updates it idempotently.
    """
    probe_cfg = _probe_state_config(state)
    target = _state_object_target(probe_cfg)
    try:
        store = build_store(probe_cfg)
    except ConfigError as exc:
        return CheckResult("state", "FAIL", str(exc))
    try:
        await store.load(_STATE_OBJECT_PROBE_KEY)
        await store.commit(
            _STATE_OBJECT_PROBE_KEY, f"sf2loki doctor probe {datetime.now(UTC).isoformat()}"
        )
    except Exception as exc:
        return CheckResult("state", "FAIL", f"probe read/write to {target} failed: {exc}")
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                await close()
    return CheckResult("state", "PASS", f"{target} is reachable and writable (doctor probe object)")


# ---------------------------------------------------------------------------
# Check 7b: coordinator
# ---------------------------------------------------------------------------


async def _check_coordinator(cfg: Config) -> CheckResult:
    """Dispatch on ``coordinate.type``: probe whichever HA coordinator is configured."""
    coord = cfg.coordinate
    if coord.type == "noop":
        return CheckResult(
            "coordinator",
            "SKIP",
            "coordinate.type is 'noop' (single instance, no HA coordinator configured)",
        )
    if coord.type == "file_lease":
        return _check_coordinator_file_lease(coord.file_lease)
    return await _check_coordinator_k8s_lease(coord.k8s_lease)


def _check_coordinator_file_lease(cfg: FileLeaseConfig) -> CheckResult:
    """Create+delete a probe file in the lease directory (never the real lease file —
    a live leader may be renewing it)."""
    lease_dir = cfg.path.parent
    probe_path = lease_dir / _COORDINATOR_LEASE_PROBE_NAME
    try:
        lease_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(probe_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.write(fd, b"sf2loki doctor coordinator probe")
            os.fsync(fd)
        finally:
            os.close(fd)
    except PermissionError as exc:
        return CheckResult(
            "coordinator",
            "FAIL",
            f"cannot write to lease directory {lease_dir}: permission denied ({exc}). "
            "file_lease requires this directory to be shared, writable storage "
            "(NFS/EFS) accessible to every replica.",
        )
    except OSError as exc:
        return CheckResult(
            "coordinator", "FAIL", f"lease directory {lease_dir} is not usable: {exc}"
        )
    finally:
        with contextlib.suppress(OSError):
            probe_path.unlink(missing_ok=True)
    return CheckResult("coordinator", "PASS", f"lease directory {lease_dir} is writable")


def _k8s_lease_probe_body(cfg: K8sLeaseConfig) -> dict[str, Any]:
    """A plain-dict Lease manifest for the dry-run create path.

    The Kubernetes python client's serializer accepts plain dicts (recursed
    via ``sanitize_for_serialization``) as well as generated model objects, so
    this avoids importing ``kubernetes_asyncio.client`` outside the lazily-
    imported default api factory — keeping this check testable with an
    injected fake ``api_factory`` and no ``k8s`` extra installed.
    """
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {"name": cfg.name},
        "spec": {
            "holderIdentity": "sf2loki-doctor-dry-run",
            "leaseDurationSeconds": int(cfg.lease_duration.total_seconds()),
        },
    }


def _k8s_status(exc: Exception) -> int | None:
    """HTTP status from a ``kubernetes_asyncio`` ``ApiException``-shaped error."""
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


def _default_k8s_api_factory(cfg: K8sLeaseConfig) -> Callable[[], AbstractAsyncContextManager[Any]]:
    """Build the real ``CoordinationV1Api`` factory (lazy import; needs the ``k8s`` extra)."""

    def _factory() -> AbstractAsyncContextManager[Any]:
        from kubernetes_asyncio import client  # type: ignore[import-not-found]
        from kubernetes_asyncio import config as k8s_config

        @contextlib.asynccontextmanager
        async def _cm() -> AsyncIterator[Any]:
            if cfg.kubeconfig is not None:
                await k8s_config.load_kube_config(config_file=str(cfg.kubeconfig))
            else:
                k8s_config.load_incluster_config()
            api_client = client.ApiClient()
            try:
                yield client.CoordinationV1Api(api_client)
            finally:
                await api_client.close()

        return _cm()

    return _factory


async def _check_coordinator_k8s_lease(
    cfg: K8sLeaseConfig,
    *,
    api_factory: Callable[[], AbstractAsyncContextManager[Any]] | None = None,
) -> CheckResult:
    """Prove get/create/update RBAC on the configured Lease without ever taking
    leadership: a plain read (get) when the Lease exists, or a server-side
    ``dry_run="All"`` create/update (validated by the API server but never
    persisted) otherwise — so this never risks creating a stray Lease or
    clobbering a live leader's lease.
    """
    if api_factory is None:
        if importlib.util.find_spec("kubernetes_asyncio") is None:
            return CheckResult(
                "coordinator",
                "FAIL",
                "coordinate.type is 'k8s_lease' but the k8s dependencies are not "
                "installed; install the extra: pip install 'sf2loki[k8s]'",
            )
        api_factory = _default_k8s_api_factory(cfg)

    lease_ref = f"{cfg.namespace}/{cfg.name}"
    try:
        async with api_factory() as api:
            try:
                lease = await api.read_namespaced_lease(name=cfg.name, namespace=cfg.namespace)
            except Exception as exc:
                if _k8s_status(exc) != 404:
                    return CheckResult(
                        "coordinator",
                        "FAIL",
                        f"cannot read Lease {lease_ref}: {exc}. The pod's ServiceAccount "
                        "needs get/create/update on leases in this namespace.",
                    )
                try:
                    await api.create_namespaced_lease(
                        namespace=cfg.namespace,
                        body=_k8s_lease_probe_body(cfg),
                        dry_run="All",
                    )
                except Exception as create_exc:
                    return CheckResult(
                        "coordinator",
                        "FAIL",
                        f"Lease {lease_ref} does not exist yet and a dry-run create "
                        f"failed: {create_exc}. The pod's ServiceAccount needs "
                        "get/create/update on leases in this namespace.",
                    )
                return CheckResult(
                    "coordinator",
                    "PASS",
                    f"Lease {lease_ref} does not exist yet; dry-run create OK "
                    "(created on first leader election)",
                )
            try:
                await api.replace_namespaced_lease(
                    name=cfg.name, namespace=cfg.namespace, body=lease, dry_run="All"
                )
            except Exception as exc:
                return CheckResult(
                    "coordinator",
                    "FAIL",
                    f"Lease {lease_ref} is readable but a dry-run update failed: {exc}. "
                    "The pod's ServiceAccount needs update on leases in this namespace.",
                )
            return CheckResult(
                "coordinator", "PASS", f"Lease {lease_ref} is reachable; get/update dry-run OK"
            )
    except Exception as exc:
        return CheckResult("coordinator", "FAIL", f"cannot reach the Kubernetes API: {exc}")


# ---------------------------------------------------------------------------
# Check 8: limits
# ---------------------------------------------------------------------------


async def _check_limits(
    sf: SalesforceConfig, tokens: TokenProvider, http_client: httpx.AsyncClient
) -> CheckResult:
    """Report DailyApiRequests remaining/max; WARN below 20% remaining."""
    client = LimitsClient(sf, tokens, http_client)
    try:
        limits = await client.fetch()
    except LimitsError as exc:
        return CheckResult("limits", "FAIL", f"org limits fetch failed: {exc}")

    info = limits.get("DailyApiRequests")
    if info is None:
        return CheckResult(
            "limits", "WARN", "DailyApiRequests not reported by the org limits endpoint"
        )
    max_requests = info["Max"]
    remaining = info["Remaining"]
    ratio = remaining / max_requests if max_requests else 1.0
    detail = f"DailyApiRequests {remaining}/{max_requests} remaining"
    if ratio < _LOW_LIMIT_RATIO:
        return CheckResult("limits", "WARN", f"{detail} (< {_LOW_LIMIT_RATIO:.0%} remaining)")
    return CheckResult("limits", "PASS", detail)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def _format_table(results: list[CheckResult]) -> str:
    name_w = max([len("name"), *(len(r.name) for r in results)])
    status_w = max([len("status"), *(len(r.status) for r in results)])
    lines = [f"{'name':<{name_w}}  {'status':<{status_w}}  detail"]
    for r in results:
        lines.append(f"{r.name:<{name_w}}  {r.status:<{status_w}}  {r.detail}")
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")
    lines.append("")
    lines.append(f"{passed} passed, {warned} warnings, {failed} failed, {skipped} skipped")
    return "\n".join(lines)


# Config-error exit code, matching cli.py's `--check`/`run`/`backfill` so the
# SAME bad config yields the SAME exit code across every subcommand (#71 item 4).
# Doctor still exits 1 for a check FAIL on a loadable config (its health-verdict
# contract); this code is used only when the config itself can't be loaded.
_CONFIG_ERROR_EXIT_CODE = 2


def _finish(results: list[CheckResult], json_output: bool, *, exit_code: int | None = None) -> int:
    if exit_code is None:
        exit_code = 1 if any(r.status == "FAIL" for r in results) else 0
    if json_output:
        payload = {"checks": [asdict(r) for r in results], "exit_code": exit_code}
        print(json.dumps(payload))
    else:
        print(_format_table(results))
    return exit_code


def _skip_remaining(results: list[CheckResult], names: Sequence[str], reason: str) -> None:
    for name in names:
        results.append(CheckResult(name, "SKIP", f"skipped: {reason}"))


# Checks 2-8, in sequence order, for the config-FAIL short-circuit.
_CHECKS_AFTER_CONFIG: tuple[str, ...] = (
    "auth",
    "permissions",
    "pubsub",
    "entitlement",
    "traceflags",
    "loki",
    "transforms",
    "telemetry",
    "state",
    "coordinator",
    "limits",
)


async def run_doctor(
    config_path: Path | None, *, json_output: bool = False, org_name: str | None = None
) -> int:
    """Run all preflight checks; return 0 (no FAIL), 1 (a check FAILed on a
    loadable config), or 2 (the config itself could not be loaded/selected —
    the same config-error code cli.py's --check/run/backfill return).

    Loads the config itself (a config problem is check #1's FAIL row, not a
    crash before the doctor starts). For a multi-org config the per-org checks
    (auth/permissions/pubsub/entitlement/limits) run against ONE org — the first
    configured, or ``--org``; a note names which. The config check validates the
    WHOLE multi-org config regardless.
    """
    results: list[CheckResult] = []

    cfg, config_result = await _check_config(config_path)
    results.append(config_result)
    if cfg is None:
        _skip_remaining(results, _CHECKS_AFTER_CONFIG, "config failed")
        return _finish(results, json_output, exit_code=_CONFIG_ERROR_EXIT_CODE)

    try:
        org, note = select_org(cfg, org_name)
    except ConfigError as exc:
        results.append(CheckResult("org", "FAIL", str(exc)))
        _skip_remaining(results, _CHECKS_AFTER_CONFIG, "org selection failed")
        return _finish(results, json_output, exit_code=_CONFIG_ERROR_EXIT_CODE)
    if note:
        results.append(CheckResult("org", "WARN", note))
    # Scope the per-org checks to the selected org (cfg.sources/salesforce -> org's).
    cfg = as_single_org_view(cfg, org)
    sf = org.salesforce

    metrics = Metrics()
    sf_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    try:
        tokens = TokenProvider(sf, sf_http, metrics=metrics)
        tok, _, auth_result = await _check_auth(sf, tokens)
        results.append(auth_result)

        if tok is None:
            _skip_remaining(
                results, ("permissions", "pubsub", "entitlement", "traceflags"), "auth failed"
            )
        else:
            results.append(await _check_permissions(cfg, sf, tok, sf_http, tokens, metrics))
            results.extend(await _check_pubsub(cfg, tokens, metrics))
            results.append(await _check_entitlement(cfg, sf, tokens, sf_http, metrics))
            results.append(await _check_traceflags(cfg, sf, tokens, sf_http, metrics))

        results.append(await _check_loki(cfg))
        results.append(_check_transforms(cfg))
        results.append(await _check_telemetry(cfg))
        results.append(await _check_state(cfg))
        results.append(await _check_coordinator(cfg))

        if tok is None:
            _skip_remaining(results, ("limits",), "auth failed")
        else:
            results.append(await _check_limits(sf, tokens, sf_http))
    finally:
        await sf_http.aclose()

    return _finish(results, json_output)
