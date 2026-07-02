"""Tests for `sf2loki doctor` (doctor.py).

Boundary mocking: respx for every real HTTP call (token endpoint, EventLogFile
describe, SOQL query, org limits, Loki push); a fake PubSubClient class
(monkeypatched into sf2loki.doctor's namespace) for the gRPC pubsub check —
see tests/salesforce/test_pubsub_client.py for get_topic's own unit tests
against a real in-process gRPC servicer.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from sf2loki import doctor as doctor_module
from sf2loki.config import Config
from sf2loki.doctor import _auth_failure_hint, run_doctor

INSTANCE_URL = "https://inst.my.salesforce.com"
LOGIN_URL = "https://myorg.my.salesforce.com"
TOKEN_URL = f"{LOGIN_URL}/services/oauth2/token"
API_VERSION = "60.0"
DESCRIBE_URL = f"{INSTANCE_URL}/services/data/v{API_VERSION}/sobjects/EventLogFile/describe"
QUERY_URL = f"{INSTANCE_URL}/services/data/v{API_VERSION}/query"
LIMITS_URL = f"{INSTANCE_URL}/services/data/v{API_VERSION}/limits"

_SAMPLE_LIMITS = {
    "DailyApiRequests": {"Max": 15000, "Remaining": 14000},
}


# ---------------------------------------------------------------------------
# Config builder
# ---------------------------------------------------------------------------


@pytest.fixture
def base_config(tmp_path: Path) -> Callable[..., Config]:
    """Factory for a Config using client_credentials (no JWT signing needed)
    against a mocked My Domain token endpoint, with org_id preset so org_id()
    never touches the network."""

    def _make(**overrides: Any) -> Config:
        base: dict[str, Any] = {
            "salesforce": {
                "client_id": "cid",
                "auth_mode": "client_credentials",
                "client_secret": "shh",
                "login_url": LOGIN_URL,
                "org_id": "00Dtest0000000",
                "api_version": API_VERSION,
            },
            "sources": {
                "pubsub": {"enabled": False},
                "eventlog_objects": {"enabled": False},
                "eventlogfile": {"enabled": False},
            },
            "sink": {"loki": {"url": "http://loki.example/loki/api/v1/push"}},
            "state": {"file": {"path": str(tmp_path / "state" / "state.json")}},
        }
        base.update(overrides)
        return Config(**base)

    return _make


def _mock_token_ok() -> None:
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "tok", "instance_url": INSTANCE_URL})
    )


def _mock_describe_ok() -> None:
    respx.get(DESCRIBE_URL).mock(return_value=httpx.Response(200, json={"name": "EventLogFile"}))


def _mock_limits_ok() -> None:
    respx.get(LIMITS_URL).mock(return_value=httpx.Response(200, json=_SAMPLE_LIMITS))


def _mock_loki_ok() -> None:
    respx.post("http://loki.example/loki/api/v1/push").mock(return_value=httpx.Response(204))


def _soql_responder(request: httpx.Request) -> httpx.Response:
    q = request.url.params.get("q", "")
    if "GROUP BY EventType" in q:
        return httpx.Response(200, json={"records": [{"EventType": "Login"}], "done": True})
    return httpx.Response(200, json={"records": [{"Id": "1"}], "done": True})


def _mock_soql_default() -> None:
    respx.get(QUERY_URL).mock(side_effect=_soql_responder)


# ---------------------------------------------------------------------------
# Fake PubSubClient (gRPC boundary)
# ---------------------------------------------------------------------------


class _FakePubSubClient:
    """Drop-in for PubSubClient.get_topic(): PASS unless the topic is "bad"."""

    def __init__(self, cfg: Any, tokens: Any, *, metrics: Any = None) -> None:
        self.closed = False
        self.requested_topics: list[str] = []

    async def get_topic(self, topic: str) -> None:
        self.requested_topics.append(topic)
        if "bad" in topic.lower():
            raise RuntimeError("NOT_FOUND: no RTEM entitlement for this channel")

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _fake_pubsub_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test gets the fake PubSubClient unless it monkeypatches its own."""
    monkeypatch.setattr(doctor_module, "PubSubClient", _FakePubSubClient)


# ---------------------------------------------------------------------------
# Happy path: everything PASSes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_all_checks_pass_returns_0(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config(
        sources={
            "pubsub": {"enabled": True, "topics": ["/event/ApexCalloutEventStream"]},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
        }
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out

    assert rc == 0
    assert "FAIL" not in out
    assert "config" in out and "PASS" in out
    assert "pubsub:/event/ApexCalloutEventStream" in out
    assert "passed" in out.splitlines()[-1]


# ---------------------------------------------------------------------------
# Helper: doctor loads config from a path, so tests write one out
# ---------------------------------------------------------------------------


def _write_yaml(cfg: Config) -> Path:
    """Dump the already-built Config back out to a temp YAML file.

    Simplest way to hand run_doctor() a path while building the Config value
    via the pydantic model (validation, defaults, coercions already applied).
    """
    import tempfile

    import yaml

    data = json.loads(cfg.model_dump_json())
    # Secrets round-trip as their plaintext value via model_dump_json's
    # SecretStr serializer set to expose value for this test-only dump.
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with open(fd, "w") as fh:
        yaml.safe_dump(data, fh)
    return Path(path)


# ---------------------------------------------------------------------------
# Config check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_config_load_error_short_circuits_and_returns_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Syntactically valid YAML, but missing the required salesforce.client_id
    # -> pydantic ValidationError -> ConfigError at load() (check #1's FAIL).
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
salesforce:
  username: svc@example.com
sink:
  loki:
    url: http://x/loki/api/v1/push
""".lstrip()
    )

    rc = await run_doctor(bad)
    out = capsys.readouterr().out

    # A config that can't even load returns the shared config-error code (2),
    # matching cli.py's --check/run/backfill (#71 item 4).
    assert rc == 2
    lines = {line.split()[0]: line for line in out.splitlines() if line.split()}
    assert "config" in lines and "FAIL" in lines["config"]
    for name in (
        "auth",
        "permissions",
        "pubsub",
        "entitlement",
        "loki",
        "transforms",
        "telemetry",
        "state",
        "coordinator",
        "limits",
    ):
        assert name in lines, f"{name} row missing"
        assert "SKIP" in lines[name]


@pytest.mark.asyncio
async def test_config_wiring_error_fails_config_check(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    """An App.build() wiring error (e.g. reserved static label) fails check #1."""
    cfg = base_config(
        sink={"loki": {"url": "http://x/loki/api/v1/push", "labels": {"source": "x"}}}
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out

    # An App.build() wiring error is the same "bad config" class → exit 2.
    assert rc == 2
    assert "config" in out
    config_line = next(line for line in out.splitlines() if line.startswith("config"))
    assert "FAIL" in config_line


# ---------------------------------------------------------------------------
# Auth check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_auth_failure_skips_sf_dependents_but_runs_loki_and_state(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(400, json={"error": "invalid_client"}))
    _mock_loki_ok()
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    lines = {line.split()[0]: line for line in out.splitlines() if line.split()}

    assert rc == 1
    assert "FAIL" in lines["auth"]
    for name in ("permissions", "pubsub", "entitlement", "limits"):
        assert "SKIP" in lines[name], f"{name} should be skipped: {lines[name]}"
    # Independent of Salesforce auth — still run.
    assert "PASS" in lines["loki"]
    assert "PASS" in lines["state"]
    assert "SKIP" in lines["transforms"]  # no hash rules configured
    assert "SKIP" in lines["telemetry"]  # telemetry disabled by default
    assert "SKIP" in lines["coordinator"]  # coordinate.type is 'noop' by default


def test_auth_failure_hint_client_credentials_generic_host() -> None:
    hint = _auth_failure_hint("client_credentials", "https://login.salesforce.com", "bad")
    assert "My Domain" in hint


def test_auth_failure_hint_jwt_invalid_grant() -> None:
    hint = _auth_failure_hint("jwt_bearer", "https://x.my.salesforce.com", "invalid_grant: x")
    assert "pre-authorised" in hint or "cert" in hint


def test_auth_failure_hint_empty_when_no_match() -> None:
    assert _auth_failure_hint("jwt_bearer", "https://x.my.salesforce.com", "some other error") == ""


# ---------------------------------------------------------------------------
# Permissions check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_permissions_403_fails(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    respx.get(DESCRIBE_URL).mock(return_value=httpx.Response(403, text="INSUFFICIENT_ACCESS"))
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("permissions"))

    assert rc == 1
    assert "FAIL" in line
    assert "View Event Log Files" in line


@pytest.mark.asyncio
@respx.mock
async def test_permissions_probes_configured_eventlog_object(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    _mock_soql_default()
    cfg = base_config(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
            "eventlogfile": {"enabled": False},
        }
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("permissions"))

    assert rc == 0
    assert "PASS" in line
    assert "LoginEvent" in line


# ---------------------------------------------------------------------------
# Pubsub check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_pubsub_disabled_is_skipped(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config()  # pubsub disabled by default

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("pubsub"))

    assert rc == 0
    assert "SKIP" in line


@pytest.mark.asyncio
@respx.mock
async def test_pubsub_wildcard_warns_without_probing(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config(
        sources={
            "pubsub": {"enabled": True, "topics": ["*"]},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
        }
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("pubsub"))

    assert rc == 0
    assert "WARN" in line
    assert "wildcard" in line


@pytest.mark.asyncio
@respx.mock
async def test_pubsub_per_topic_grpc_error_fails_only_that_topic(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config(
        sources={
            "pubsub": {
                "enabled": True,
                "topics": ["/event/GoodEventStream", "/event/BadEventStream"],
            },
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
        }
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    good_line = next(
        ln for ln in out.splitlines() if ln.startswith("pubsub:/event/GoodEventStream")
    )
    bad_line = next(ln for ln in out.splitlines() if ln.startswith("pubsub:/event/BadEventStream"))

    assert rc == 1
    assert "PASS" in good_line
    assert "FAIL" in bad_line
    assert "NOT_FOUND" in bad_line


# ---------------------------------------------------------------------------
# Entitlement check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_entitlement_disabled_is_skipped(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("entitlement"))

    assert rc == 0
    assert "SKIP" in line


@pytest.mark.asyncio
@respx.mock
async def test_entitlement_warns_for_type_with_no_files(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    # _soql_responder's GROUP BY branch returns only "Login" as produced.
    respx.get(QUERY_URL).mock(side_effect=_soql_responder)
    cfg = base_config(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": True, "event_types": ["Login", "ReportExport"]},
        }
    )

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("entitlement"))

    assert rc == 0  # WARN, not FAIL
    assert "WARN" in line
    assert "ReportExport" in line


# ---------------------------------------------------------------------------
# Loki check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_loki_push_pass_reports_latency(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("loki"))

    assert rc == 0
    assert "PASS" in line
    assert "ms" in line
    assert "only write" in line


@pytest.mark.asyncio
@respx.mock
async def test_loki_401_fails_with_tenant_hint(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    respx.post("http://loki.example/loki/api/v1/push").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("loki"))

    assert rc == 1
    assert "FAIL" in line
    assert "tenant_id" in line


# ---------------------------------------------------------------------------
# State check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_state_dir_probe_passes_and_cleans_up(
    base_config: Callable[..., Config], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    state_dir = tmp_path / "state"
    cfg = base_config(state={"file": {"path": str(state_dir / "state.json")}})

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("state"))

    assert rc == 0
    assert "PASS" in line
    assert not (state_dir / ".sf2loki-doctor-probe").exists()


@pytest.mark.asyncio
@respx.mock
async def test_state_dir_permission_denied_fails(
    base_config: Callable[..., Config], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    state_dir = tmp_path / "locked"
    state_dir.mkdir()
    state_dir.chmod(0o000)
    cfg = base_config(state={"file": {"path": str(state_dir / "state.json")}})

    try:
        rc = await run_doctor(_write_yaml(cfg))
    finally:
        state_dir.chmod(0o700)  # restore so tmp_path cleanup can remove it
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("state"))

    assert rc == 1
    assert "FAIL" in line
    assert "uid 10001" in line


# ---------------------------------------------------------------------------
# State check — s3/gcs backends (issue #59)
# ---------------------------------------------------------------------------


class _FakeCheckpointStore:
    """Drop-in for CheckpointStore.load/commit (+ optional close) — lets the
    s3/gcs state probe be tested without aiobotocore/gcloud-aio-storage
    installed (mirrors the "fake PubSubClient" boundary-mocking approach used
    for the gRPC pubsub check elsewhere in this file)."""

    def __init__(
        self, *, existing: dict[str, str] | None = None, commit_error: Exception | None = None
    ) -> None:
        self.data = dict(existing or {})
        self.commit_error = commit_error
        self.closed = False
        self.loaded_keys: list[str] = []
        self.committed: list[tuple[str, str]] = []

    async def load(self, key: str) -> str | None:
        self.loaded_keys.append(key)
        return self.data.get(key)

    async def commit(self, key: str, value: str) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.committed.append((key, value))
        self.data[key] = value

    async def close(self) -> None:
        self.closed = True


def test_probe_state_config_suffixes_s3_key_not_real_key() -> None:
    from sf2loki.config import S3StateConfig, StateConfig

    state = StateConfig(store="s3", s3=S3StateConfig(bucket="b", key="sf2loki/state.json"))
    probe = doctor_module._probe_state_config(state)

    assert probe.s3.key != state.s3.key
    assert probe.s3.bucket == state.s3.bucket


def test_probe_state_config_suffixes_gcs_object_not_real_object() -> None:
    from sf2loki.config import GcsStateConfig, StateConfig

    state = StateConfig(
        store="gcs", gcs=GcsStateConfig(bucket="b", object_name="sf2loki/state.json")
    )
    probe = doctor_module._probe_state_config(state)

    assert probe.gcs.object_name != state.gcs.object_name
    assert probe.gcs.bucket == state.gcs.bucket


def test_probe_state_config_leaves_file_backend_unchanged() -> None:
    from sf2loki.config import StateConfig

    state = StateConfig(store="file")
    probe = doctor_module._probe_state_config(state)

    assert probe.file.path == state.file.path


@pytest.mark.asyncio
async def test_check_state_object_s3_passes_and_probes_doctor_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sf2loki.config import S3StateConfig, StateConfig

    state = StateConfig(store="s3", s3=S3StateConfig(bucket="b", key="sf2loki/state.json"))
    fake_store = _FakeCheckpointStore()
    seen_cfgs: list[StateConfig] = []

    def fake_build_store(cfg: StateConfig) -> _FakeCheckpointStore:
        seen_cfgs.append(cfg)
        return fake_store

    monkeypatch.setattr(doctor_module, "build_store", fake_build_store)

    result = await doctor_module._check_state_object(state)

    assert result.name == "state"
    assert result.status == "PASS"
    assert seen_cfgs[0].s3.key != state.s3.key  # never the real key
    assert fake_store.loaded_keys == ["doctor"]
    assert fake_store.committed[0][0] == "doctor"
    assert fake_store.closed is True
    assert "s3://b/" in result.detail


@pytest.mark.asyncio
async def test_check_state_object_gcs_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    from sf2loki.config import GcsStateConfig, StateConfig

    state = StateConfig(
        store="gcs", gcs=GcsStateConfig(bucket="b", object_name="sf2loki/state.json")
    )
    fake_store = _FakeCheckpointStore()
    monkeypatch.setattr(doctor_module, "build_store", lambda cfg: fake_store)

    result = await doctor_module._check_state_object(state)

    assert result.status == "PASS"
    assert "gs://b/" in result.detail


@pytest.mark.asyncio
async def test_check_state_object_missing_extra_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from sf2loki.config import ConfigError, S3StateConfig, StateConfig

    state = StateConfig(store="s3", s3=S3StateConfig(bucket="b"))

    def fake_build_store(cfg: StateConfig) -> Any:
        raise ConfigError("state.store is 's3' ... install the extra: pip install 'sf2loki[s3]'")

    monkeypatch.setattr(doctor_module, "build_store", fake_build_store)

    result = await doctor_module._check_state_object(state)

    assert result.status == "FAIL"
    assert "sf2loki[s3]" in result.detail


@pytest.mark.asyncio
async def test_check_state_object_commit_failure_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from sf2loki.config import S3StateConfig, StateConfig

    state = StateConfig(store="s3", s3=S3StateConfig(bucket="b"))
    fake_store = _FakeCheckpointStore(commit_error=RuntimeError("access denied"))
    monkeypatch.setattr(doctor_module, "build_store", lambda cfg: fake_store)

    result = await doctor_module._check_state_object(state)

    assert result.status == "FAIL"
    assert "access denied" in result.detail
    assert fake_store.closed is True  # cleanup still runs on failure


@pytest.mark.asyncio
async def test_check_state_dispatches_to_object_for_s3(
    base_config: Callable[..., Config], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_store = _FakeCheckpointStore()
    monkeypatch.setattr(doctor_module, "build_store", lambda cfg: fake_store)
    cfg = base_config(state={"store": "s3", "s3": {"bucket": "b"}})

    result = await doctor_module._check_state(cfg)

    assert result.name == "state"
    assert result.status == "PASS"


@pytest.mark.asyncio
@respx.mock
async def test_state_check_wired_for_s3_via_run_doctor(
    base_config: Callable[..., Config],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: run_doctor's state row actually probes the configured s3
    backend rather than the (unused) default local state directory."""
    from sf2loki import app as app_module

    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    fake_store = _FakeCheckpointStore()
    monkeypatch.setattr(doctor_module, "build_store", lambda cfg: fake_store)
    # App.build() (called by check #1) also constructs the real store to
    # validate wiring — patch its reference too so this test doesn't need the
    # sf2loki[s3] extra installed.
    monkeypatch.setattr(app_module, "build_store", lambda cfg, **kw: fake_store)
    cfg = base_config(state={"store": "s3", "s3": {"bucket": "b"}})

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("state"))

    assert rc == 0
    assert "PASS" in line
    assert "s3://b/" in line


# ---------------------------------------------------------------------------
# Transforms check (issue #67)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transforms_check_skips_when_no_hash_rules(
    base_config: Callable[..., Config],
) -> None:
    cfg = base_config()
    result = doctor_module._check_transforms(cfg)
    assert result.status == "SKIP"


@pytest.mark.asyncio
async def test_transforms_check_warns_on_unsalted_hash_rule(
    base_config: Callable[..., Config],
) -> None:
    cfg = base_config(
        sources={
            "pubsub": {
                "enabled": True,
                "topics": ["/event/LoginEventStream"],
                "transforms": [{"action": "hash", "fields": ["SOURCE_IP"]}],
            },
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
        }
    )
    result = doctor_module._check_transforms(cfg)
    assert result.status == "WARN"
    assert "SOURCE_IP" in result.detail


@pytest.mark.asyncio
async def test_transforms_check_passes_when_hash_rule_salted(
    base_config: Callable[..., Config],
) -> None:
    cfg = base_config(
        sources={
            "pubsub": {
                "enabled": True,
                "topics": ["/event/LoginEventStream"],
                "transforms": [{"action": "hash", "fields": ["SOURCE_IP"]}],
            },
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
            "transform_salt": "s3cret",
        }
    )
    result = doctor_module._check_transforms(cfg)
    assert result.status == "PASS"


# ---------------------------------------------------------------------------
# Telemetry check (issue #59)
# ---------------------------------------------------------------------------

OTLP_URL = "http://otlp.example/v1/metrics"


@pytest.mark.asyncio
async def test_telemetry_disabled_is_skipped(base_config: Callable[..., Config]) -> None:
    cfg = base_config()
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "SKIP"


@pytest.mark.asyncio
async def test_telemetry_enabled_without_endpoint_fails(
    base_config: Callable[..., Config],
) -> None:
    cfg = base_config(service={"telemetry": {"enabled": True, "endpoint": ""}})
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "FAIL"


@pytest.mark.asyncio
@respx.mock
async def test_telemetry_pass_when_endpoint_reachable(
    base_config: Callable[..., Config],
) -> None:
    respx.post(OTLP_URL).mock(return_value=httpx.Response(200))
    cfg = base_config(
        service={"telemetry": {"enabled": True, "endpoint": OTLP_URL, "auth": "none"}}
    )
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "PASS"


@pytest.mark.asyncio
@respx.mock
async def test_telemetry_401_fails_with_hint(base_config: Callable[..., Config]) -> None:
    respx.post(OTLP_URL).mock(return_value=httpx.Response(401, text="denied"))
    cfg = base_config(
        service={"telemetry": {"enabled": True, "endpoint": OTLP_URL, "auth": "none"}}
    )
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "FAIL"
    assert "tenant" in result.detail or "instance id" in result.detail


@pytest.mark.asyncio
@respx.mock
async def test_telemetry_non_success_fails(base_config: Callable[..., Config]) -> None:
    respx.post(OTLP_URL).mock(return_value=httpx.Response(500, text="oops"))
    cfg = base_config(
        service={"telemetry": {"enabled": True, "endpoint": OTLP_URL, "auth": "none"}}
    )
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "FAIL"


@pytest.mark.asyncio
@respx.mock
async def test_telemetry_unreachable_fails(base_config: Callable[..., Config]) -> None:
    respx.post(OTLP_URL).mock(side_effect=httpx.ConnectError("boom"))
    cfg = base_config(
        service={"telemetry": {"enabled": True, "endpoint": OTLP_URL, "auth": "none"}}
    )
    result = await doctor_module._check_telemetry(cfg)
    assert result.status == "FAIL"
    assert "unreachable" in result.detail


# ---------------------------------------------------------------------------
# Coordinator check (issue #59)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coordinator_noop_is_skipped(base_config: Callable[..., Config]) -> None:
    cfg = base_config()  # coordinate.type defaults to "noop"
    result = await doctor_module._check_coordinator(cfg)
    assert result.status == "SKIP"


def test_coordinator_file_lease_dir_writable_passes_and_cleans_up(tmp_path: Path) -> None:
    from sf2loki.config import FileLeaseConfig

    lease_dir = tmp_path / "lease"
    cfg = FileLeaseConfig(path=lease_dir / "leader.lease")

    result = doctor_module._check_coordinator_file_lease(cfg)

    assert result.status == "PASS"
    assert not (lease_dir / doctor_module._COORDINATOR_LEASE_PROBE_NAME).exists()


def test_coordinator_file_lease_dir_permission_denied_fails(tmp_path: Path) -> None:
    from sf2loki.config import FileLeaseConfig

    lease_dir = tmp_path / "locked"
    lease_dir.mkdir()
    lease_dir.chmod(0o000)
    cfg = FileLeaseConfig(path=lease_dir / "leader.lease")

    try:
        result = doctor_module._check_coordinator_file_lease(cfg)
    finally:
        lease_dir.chmod(0o700)  # restore so tmp_path cleanup can remove it

    assert result.status == "FAIL"
    assert "shared, writable storage" in result.detail


@pytest.mark.asyncio
async def test_coordinator_dispatches_to_file_lease(
    base_config: Callable[..., Config], tmp_path: Path
) -> None:
    lease_path = tmp_path / "lease" / "leader.lease"
    cfg = base_config(coordinate={"type": "file_lease", "file_lease": {"path": str(lease_path)}})

    result = await doctor_module._check_coordinator(cfg)

    assert result.status == "PASS"


class _FakeK8sApiException(Exception):
    """Duck-typed like kubernetes_asyncio's ApiException: exposes .status."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


class _FakeK8sApi:
    """Fake CoordinationV1Api: records dry_run/body on create/replace calls."""

    def __init__(
        self,
        *,
        existing_lease: Any = "SENTINEL_LEASE",
        read_error: Exception | None = None,
        create_error: Exception | None = None,
        replace_error: Exception | None = None,
    ) -> None:
        self.existing_lease = existing_lease
        self.read_error = read_error
        self.create_error = create_error
        self.replace_error = replace_error
        self.create_calls: list[dict[str, Any]] = []
        self.replace_calls: list[dict[str, Any]] = []

    async def read_namespaced_lease(self, *, name: str, namespace: str) -> Any:
        if self.read_error is not None:
            raise self.read_error
        return self.existing_lease

    async def create_namespaced_lease(self, *, namespace: str, body: Any, dry_run: str) -> Any:
        self.create_calls.append({"namespace": namespace, "body": body, "dry_run": dry_run})
        if self.create_error is not None:
            raise self.create_error
        return body

    async def replace_namespaced_lease(
        self, *, name: str, namespace: str, body: Any, dry_run: str
    ) -> Any:
        self.replace_calls.append(
            {"name": name, "namespace": namespace, "body": body, "dry_run": dry_run}
        )
        if self.replace_error is not None:
            raise self.replace_error
        return body


def _fake_k8s_api_factory(api: _FakeK8sApi) -> Callable[[], Any]:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _cm() -> Any:
        yield api

    return lambda: _cm()


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_missing_extra_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from sf2loki.config import K8sLeaseConfig

    monkeypatch.setattr(doctor_module.importlib.util, "find_spec", lambda name: None)
    cfg = K8sLeaseConfig()

    result = await doctor_module._check_coordinator_k8s_lease(cfg)

    assert result.status == "FAIL"
    assert "sf2loki[k8s]" in result.detail


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_exists_dry_run_update_passes() -> None:
    from sf2loki.config import K8sLeaseConfig

    api = _FakeK8sApi()
    cfg = K8sLeaseConfig(namespace="ns", name="sf2loki-leader")

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "PASS"
    assert api.replace_calls
    assert api.replace_calls[0]["dry_run"] == "All"
    assert api.replace_calls[0]["body"] == "SENTINEL_LEASE"  # replays the exact lease read back
    assert not api.create_calls


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_missing_dry_run_create_passes() -> None:
    from sf2loki.config import K8sLeaseConfig

    api = _FakeK8sApi(read_error=_FakeK8sApiException(404))
    cfg = K8sLeaseConfig(namespace="ns", name="sf2loki-leader")

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "PASS"
    assert api.create_calls
    assert api.create_calls[0]["dry_run"] == "All"
    assert api.create_calls[0]["body"]["metadata"]["name"] == "sf2loki-leader"
    assert "does not exist yet" in result.detail


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_read_403_fails() -> None:
    from sf2loki.config import K8sLeaseConfig

    api = _FakeK8sApi(read_error=_FakeK8sApiException(403))
    cfg = K8sLeaseConfig(namespace="ns", name="sf2loki-leader")

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "FAIL"
    assert "get/create/update" in result.detail


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_dry_run_create_failure_fails() -> None:
    from sf2loki.config import K8sLeaseConfig

    api = _FakeK8sApi(read_error=_FakeK8sApiException(404), create_error=_FakeK8sApiException(403))
    cfg = K8sLeaseConfig(namespace="ns", name="sf2loki-leader")

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "FAIL"


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_dry_run_update_failure_fails() -> None:
    from sf2loki.config import K8sLeaseConfig

    api = _FakeK8sApi(replace_error=_FakeK8sApiException(403))
    cfg = K8sLeaseConfig(namespace="ns", name="sf2loki-leader")

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "FAIL"


@pytest.mark.asyncio
async def test_coordinator_k8s_lease_dispatches_from_check_coordinator(
    base_config: Callable[..., Config],
) -> None:
    api = _FakeK8sApi()
    cfg = base_config(coordinate={"type": "k8s_lease", "k8s_lease": {"namespace": "ns"}})

    result = await doctor_module._check_coordinator_k8s_lease(
        cfg.coordinate.k8s_lease, api_factory=_fake_k8s_api_factory(api)
    )

    assert result.status == "PASS"


# ---------------------------------------------------------------------------
# Limits check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_limits_warns_when_remaining_low(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_loki_ok()
    respx.get(LIMITS_URL).mock(
        return_value=httpx.Response(
            200, json={"DailyApiRequests": {"Max": 15000, "Remaining": 100}}
        )
    )
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("limits"))

    assert rc == 0  # WARN, not FAIL
    assert "WARN" in line


@pytest.mark.asyncio
@respx.mock
async def test_limits_pass_when_remaining_healthy(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_loki_ok()
    _mock_limits_ok()
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if ln.startswith("limits"))

    assert rc == 0
    assert "PASS" in line


# ---------------------------------------------------------------------------
# --json output
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_json_output_shape(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    cfg = base_config()

    rc = await run_doctor(_write_yaml(cfg), json_output=True)
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert rc == 0
    assert payload["exit_code"] == 0
    names = [c["name"] for c in payload["checks"]]
    assert "config" in names
    assert "auth" in names
    assert all({"name", "status", "detail"} <= set(c) for c in payload["checks"])


@pytest.mark.asyncio
async def test_json_output_on_config_failure(capsys: pytest.CaptureFixture[str]) -> None:
    rc = await run_doctor(Path("/does/not/exist.yaml"), json_output=True)
    out = capsys.readouterr().out
    payload = json.loads(out)

    # config-load failure → shared config-error code (2), and the JSON payload's
    # embedded exit_code matches the process exit (#71 item 4).
    assert rc == 2
    assert payload["exit_code"] == 2
    config_check = next(c for c in payload["checks"] if c["name"] == "config")
    assert config_check["status"] == "FAIL"


# ---------------------------------------------------------------------------
# TraceFlags check (apexlog)
# ---------------------------------------------------------------------------

TOOLING_QUERY_URL = f"{INSTANCE_URL}/services/data/v{API_VERSION}/tooling/query"


@pytest.mark.asyncio
@respx.mock
async def test_traceflags_skipped_when_apexlog_disabled(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    rc = await run_doctor(_write_yaml(base_config()))
    out = capsys.readouterr().out
    assert "traceflags" in out
    assert "apexlog source disabled" in out
    assert rc == 0


@pytest.mark.asyncio
@respx.mock
async def test_traceflags_warns_when_none_active(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    respx.get(TOOLING_QUERY_URL).mock(
        return_value=httpx.Response(200, json={"records": [], "done": True})
    )
    cfg = base_config(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
            "apexlog": {"enabled": True},
        }
    )
    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    assert "traceflags" in out
    assert "WARN" in out
    assert "TraceFlag" in out
    assert rc == 0  # WARN never fails the run


@pytest.mark.asyncio
@respx.mock
async def test_traceflags_pass_when_active(
    base_config: Callable[..., Config], capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_token_ok()
    _mock_describe_ok()
    _mock_limits_ok()
    _mock_loki_ok()
    respx.get(TOOLING_QUERY_URL).mock(
        return_value=httpx.Response(200, json={"records": [{"Id": "7tf1"}], "done": True})
    )
    cfg = base_config(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
            "apexlog": {"enabled": True},
        }
    )
    rc = await run_doctor(_write_yaml(cfg))
    out = capsys.readouterr().out
    assert "traceflags" in out
    assert "1 active" in out
    assert rc == 0
