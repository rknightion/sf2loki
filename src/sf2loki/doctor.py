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
import json
import os
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx

from sf2loki.app import App
from sf2loki.auth.jwt_auth import AccessToken, AuthError, TokenProvider
from sf2loki.config import (
    EVENT_TYPE_WILDCARD,
    Config,
    ConfigError,
    PubSubConfig,
    SalesforceConfig,
    as_single_org_view,
    load,
    select_org,
)
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import EventLogFileClient, EventLogFileError
from sf2loki.salesforce.limits_client import LimitsClient, LimitsError
from sf2loki.salesforce.pubsub_client import PubSubClient
from sf2loki.salesforce.soql_client import SoqlClient, SoqlError
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink

# Explicit HTTP timeouts for the shared clients this command owns — mirrors
# App.build's _HTTP_TIMEOUT (httpx's 5s-everywhere default churns on slow
# Salesforce responses / large-ish Loki test pushes). Kept as an independent
# constant rather than importing app.py's private one so this file has no
# coupling to App's internals beyond the App.build() call itself.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)

# Probe file name for the "state" check — deliberately NOT the real state
# file name: the running daemon may hold an flock on that one.
_STATE_PROBE_NAME = ".sf2loki-doctor-probe"

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
# Check 7: state
# ---------------------------------------------------------------------------


def _check_state(cfg: Config) -> CheckResult:
    """Create+lock+delete a probe file in the state directory (never the real state file)."""
    state_dir = cfg.state.file.path.parent
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


def _finish(results: list[CheckResult], json_output: bool) -> int:
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
    "loki",
    "state",
    "limits",
)


async def run_doctor(
    config_path: Path | None, *, json_output: bool = False, org_name: str | None = None
) -> int:
    """Run all preflight checks; return 0 (no FAIL) or 1 (any FAIL).

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
        return _finish(results, json_output)

    try:
        org, note = select_org(cfg, org_name)
    except ConfigError as exc:
        results.append(CheckResult("org", "FAIL", str(exc)))
        _skip_remaining(results, _CHECKS_AFTER_CONFIG, "org selection failed")
        return _finish(results, json_output)
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
            _skip_remaining(results, ("permissions", "pubsub", "entitlement"), "auth failed")
        else:
            results.append(await _check_permissions(cfg, sf, tok, sf_http, tokens, metrics))
            results.extend(await _check_pubsub(cfg, tokens, metrics))
            results.append(await _check_entitlement(cfg, sf, tokens, sf_http, metrics))

        results.append(await _check_loki(cfg))
        results.append(_check_state(cfg))

        if tok is None:
            _skip_remaining(results, ("limits",), "auth failed")
        else:
            results.append(await _check_limits(sf, tokens, sf_http))
    finally:
        await sf_http.aclose()

    return _finish(results, json_output)
