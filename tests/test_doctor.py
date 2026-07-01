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
async def test_config_load_error_short_circuits_and_returns_1(
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

    assert rc == 1
    lines = {line.split()[0]: line for line in out.splitlines() if line.split()}
    assert "config" in lines and "FAIL" in lines["config"]
    for name in ("auth", "permissions", "pubsub", "entitlement", "loki", "state", "limits"):
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

    assert rc == 1
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

    assert rc == 1
    assert payload["exit_code"] == 1
    config_check = next(c for c in payload["checks"] if c["name"] == "config")
    assert config_check["status"] == "FAIL"
