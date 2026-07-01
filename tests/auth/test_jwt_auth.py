"""Tests for Salesforce OAuth 2.0 JWT bearer flow (auth/jwt_auth.py)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from sf2loki.auth.jwt_auth import AccessToken, AuthError, TokenProvider
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def rsa_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture()
def private_key_pem(rsa_private_key: rsa.RSAPrivateKey) -> str:
    return rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture()
def sf_config(private_key_pem: str) -> SalesforceConfig:
    return SalesforceConfig(
        login_url="https://login.salesforce.com",
        client_id="myclientid",
        username="svc@example.com",
        private_key=SecretStr(private_key_pem),
        api_version="60.0",
    )


@pytest.fixture()
def sf_config_with_org_id(private_key_pem: str) -> SalesforceConfig:
    return SalesforceConfig(
        login_url="https://login.salesforce.com",
        client_id="myclientid",
        username="svc@example.com",
        private_key=SecretStr(private_key_pem),
        api_version="60.0",
        org_id="00Dxxxxxxxxxxxxxxx",
    )


@pytest.fixture()
def cc_config() -> SalesforceConfig:
    # client_credentials requires the org's My Domain token endpoint (the
    # generic login/test hosts reject the grant, enforced at config time).
    return SalesforceConfig(
        login_url="https://myorg.my.salesforce.com",
        auth_mode="client_credentials",
        client_id="myclientid",
        client_secret=SecretStr("mysecret"),
    )


TOKEN_ENDPOINT = "https://login.salesforce.com/services/oauth2/token"
CC_TOKEN_ENDPOINT = "https://myorg.my.salesforce.com/services/oauth2/token"
INSTANCE_URL = "https://myorg.my.salesforce.com"
ACCESS_TOKEN_VALUE = "dummy_access_token_12345"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _token_response(
    access_token: str = ACCESS_TOKEN_VALUE,
    instance_url: str = INSTANCE_URL,
) -> dict[str, str]:
    return {"access_token": access_token, "instance_url": instance_url}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@respx.mock
async def test_token_posts_to_endpoint_and_returns_access_token(
    sf_config: SalesforceConfig,
    rsa_private_key: rsa.RSAPrivateKey,
) -> None:
    """token() POSTs a JWT bearer assertion and returns a populated AccessToken."""
    route = respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json=_token_response())
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        token = await provider.token()

    assert isinstance(token, AccessToken)
    assert token.value == ACCESS_TOKEN_VALUE
    assert token.instance_url == INSTANCE_URL
    assert isinstance(token.expires_at, datetime)
    assert token.expires_at.tzinfo is not None  # timezone-aware

    # Verify the outbound request form fields
    assert route.called
    request = route.calls.last.request
    body = dict(pair.split("=", 1) for pair in request.content.decode().split("&") if "=" in pair)
    assert body.get("grant_type") == "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
    assert "assertion" in body

    # Decode and verify the JWT assertion claims
    import urllib.parse

    assertion_jwt = urllib.parse.unquote(body["assertion"])
    public_key = rsa_private_key.public_key()
    claims = jwt.decode(
        assertion_jwt,
        public_key,
        algorithms=["RS256"],
        audience=sf_config.login_url,
    )
    assert claims["iss"] == sf_config.client_id
    assert claims["sub"] == sf_config.username
    assert claims["aud"] == sf_config.login_url


@respx.mock
async def test_client_credentials_posts_client_credentials_grant(
    cc_config: SalesforceConfig,
) -> None:
    """client_credentials mode POSTs a client_credentials grant (no JWT assertion)."""
    route = respx.post(CC_TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json=_token_response())
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(cc_config, client)
        token = await provider.token()

    assert token.value == ACCESS_TOKEN_VALUE
    assert token.instance_url == INSTANCE_URL

    request = route.calls.last.request
    body = dict(p.split("=", 1) for p in request.content.decode().split("&") if "=" in p)
    assert body.get("grant_type") == "client_credentials"
    assert body.get("client_id") == "myclientid"
    assert body.get("client_secret") == "mysecret"
    assert "assertion" not in body  # no JWT minted in this mode


@respx.mock
async def test_client_credentials_caches_and_refreshes(cc_config: SalesforceConfig) -> None:
    """client_credentials reuses the shared cache/invalidate path."""
    route = respx.post(CC_TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json=_token_response())
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(cc_config, client)
        t1 = await provider.token()
        t2 = await provider.token()
        assert t1 is t2
        assert route.call_count == 1
        provider.invalidate()
        await provider.token()
        assert route.call_count == 2


@respx.mock
async def test_token_is_cached_on_second_call(sf_config: SalesforceConfig) -> None:
    """A second token() call does not make a second HTTP request."""
    route = respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json=_token_response())
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        token1 = await provider.token()
        token2 = await provider.token()

    assert token1 is token2
    assert route.call_count == 1


@respx.mock
async def test_invalidate_forces_re_request(sf_config: SalesforceConfig) -> None:
    """After invalidate(), the next token() call makes a fresh HTTP request."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        token1 = await provider.token()
        provider.invalidate()
        token2 = await provider.token()

    # Two separate AccessToken objects (re-requested)
    assert token1 is not token2
    assert respx.calls.call_count == 2


@respx.mock
async def test_org_id_returns_config_value_without_http(
    sf_config_with_org_id: SalesforceConfig,
) -> None:
    """org_id() returns cfg.org_id when set, without making any HTTP request."""
    # No mocked routes — any HTTP call would raise
    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config_with_org_id, client)
        org_id = await provider.org_id()

    assert org_id == "00Dxxxxxxxxxxxxxxx"


@respx.mock
async def test_org_id_fetches_from_userinfo_when_not_configured(
    sf_config: SalesforceConfig,
) -> None:
    """org_id() GETs userinfo and returns organization_id when cfg.org_id is None."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    userinfo_url = f"{INSTANCE_URL}/services/oauth2/userinfo"
    respx.get(userinfo_url).mock(
        return_value=httpx.Response(
            200,
            json={"organization_id": "00Dabc000000def", "user_id": "005xxx"},
        )
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        org_id = await provider.org_id()

    assert org_id == "00Dabc000000def"


@respx.mock
async def test_org_id_is_cached_on_second_call(sf_config: SalesforceConfig) -> None:
    """org_id() caches the resolved value — second call makes no HTTP request."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    userinfo_url = f"{INSTANCE_URL}/services/oauth2/userinfo"
    userinfo_route = respx.get(userinfo_url).mock(
        return_value=httpx.Response(
            200,
            json={"organization_id": "00Dabc000000def"},
        )
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        first = await provider.org_id()
        second = await provider.org_id()

    assert first == second == "00Dabc000000def"
    assert userinfo_route.call_count == 1


@respx.mock
async def test_token_retries_on_500_then_succeeds(sf_config: SalesforceConfig) -> None:
    """A 500 from the token endpoint is retried; success on second attempt works."""
    respx.post(TOKEN_ENDPOINT).mock(
        side_effect=[
            httpx.Response(500, text="Server Error"),
            httpx.Response(200, json=_token_response()),
        ]
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        token = await provider.token()

    assert token.value == ACCESS_TOKEN_VALUE
    assert respx.calls.call_count == 2


@respx.mock
async def test_token_raises_auth_error_immediately_on_400(sf_config: SalesforceConfig) -> None:
    """A 400 from the token endpoint raises AuthError immediately (no retry)."""
    respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(
            400,
            json={"error": "invalid_grant", "error_description": "Bad JWT"},
        )
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        with pytest.raises(AuthError):
            await provider.token()

    # Only one call — no retry on 4xx
    assert respx.calls.call_count == 1


@respx.mock
async def test_token_raises_auth_error_immediately_on_401(sf_config: SalesforceConfig) -> None:
    """A 401 from the token endpoint raises AuthError immediately (no retry)."""
    respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(401, json={"error": "unauthorized"})
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        with pytest.raises(AuthError):
            await provider.token()

    assert respx.calls.call_count == 1


@respx.mock
async def test_access_token_dataclass_fields() -> None:
    """AccessToken is a frozen dataclass with value, instance_url, expires_at."""
    now = datetime.now(UTC)
    token = AccessToken(
        value="tok",
        instance_url="https://example.my.salesforce.com",
        expires_at=now + timedelta(hours=1),
    )
    assert token.value == "tok"
    assert token.instance_url == "https://example.my.salesforce.com"
    assert token.expires_at > now

    # frozen: assigning should raise
    with pytest.raises((AttributeError, TypeError)):
        token.value = "other"  # type: ignore[misc]


@respx.mock
async def test_concurrent_token_calls_mint_only_once(sf_config: SalesforceConfig) -> None:
    """Concurrent token() calls must not double-mint (lock guards minting)."""
    import asyncio

    route = respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json=_token_response())
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        tokens = await asyncio.gather(
            provider.token(),
            provider.token(),
            provider.token(),
        )

    # All return the same token, only one HTTP call made
    assert all(t is tokens[0] for t in tokens)
    assert route.call_count == 1


@respx.mock
async def test_jwt_exp_claim_is_near_future(
    sf_config: SalesforceConfig,
    rsa_private_key: rsa.RSAPrivateKey,
) -> None:
    """The minted JWT exp claim is roughly now+180s (within ±10s tolerance)."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))

    before = int(time.time())

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        await provider.token()

    request = respx.calls.last.request
    import urllib.parse

    body = dict(p.split("=", 1) for p in request.content.decode().split("&") if "=" in p)
    assertion_jwt = urllib.parse.unquote(body["assertion"])
    public_key = rsa_private_key.public_key()
    claims = jwt.decode(
        assertion_jwt,
        public_key,
        algorithms=["RS256"],
        audience=sf_config.login_url,
    )
    after = int(time.time())

    # exp should be ~180s from minting time
    assert before + 170 <= claims["exp"] <= after + 190


@respx.mock
async def test_successful_mint_increments_auth_refreshes(sf_config: SalesforceConfig) -> None:
    """A successful token mint increments the auth_refreshes counter."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    metrics = Metrics()

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client, metrics=metrics)
        await provider.token()

    assert metrics.registry.get_sample_value("sf2loki_auth_refreshes_total") == 1.0


@respx.mock
async def test_auth_error_increments_auth_errors(sf_config: SalesforceConfig) -> None:
    """A non-retryable token-endpoint error increments the auth_errors counter."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(400, json={"error": "bad"}))
    metrics = Metrics()

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client, metrics=metrics)
        with pytest.raises(AuthError):
            await provider.token()

    assert metrics.registry.get_sample_value("sf2loki_auth_errors_total") == 1.0


@respx.mock
async def test_metrics_defaults_to_a_private_registry_when_omitted(
    sf_config: SalesforceConfig,
) -> None:
    """No metrics passed in -> a default Metrics() is used internally; no crash."""
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        token = await provider.token()

    assert token.value == ACCESS_TOKEN_VALUE


# ---------------------------------------------------------------------------
# Configurable token TTL (D7a): assumed expiry = salesforce.token_ttl


@respx.mock
async def test_token_ttl_from_config_drives_expiry(private_key_pem: str) -> None:
    """expires_at honours salesforce.token_ttl (org session timeout can be 15m)."""
    cfg = SalesforceConfig(
        login_url="https://login.salesforce.com",
        client_id="myclientid",
        username="svc@example.com",
        private_key=SecretStr(private_key_pem),
        token_ttl=timedelta(minutes=15),
    )
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))

    before = datetime.now(UTC)
    async with httpx.AsyncClient() as client:
        provider = TokenProvider(cfg, client)
        token = await provider.token()
    after = datetime.now(UTC)

    assert before + timedelta(minutes=14) < token.expires_at <= after + timedelta(minutes=15)


# ---------------------------------------------------------------------------
# userinfo retry policy (D7b): transient 5xx retried, 4xx fail fast


@respx.mock
async def test_org_id_userinfo_retries_on_500_then_succeeds(
    sf_config: SalesforceConfig,
) -> None:
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    userinfo_url = f"{INSTANCE_URL}/services/oauth2/userinfo"
    userinfo_route = respx.get(userinfo_url).mock(
        side_effect=[
            httpx.Response(500, text="Server Error"),
            httpx.Response(200, json={"organization_id": "00Dabc000000def"}),
        ]
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        org_id = await provider.org_id()

    assert org_id == "00Dabc000000def"
    assert userinfo_route.call_count == 2


@respx.mock
async def test_org_id_userinfo_retries_on_transport_error(
    sf_config: SalesforceConfig,
) -> None:
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    userinfo_url = f"{INSTANCE_URL}/services/oauth2/userinfo"
    userinfo_route = respx.get(userinfo_url).mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200, json={"organization_id": "00Dabc000000def"}),
        ]
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        org_id = await provider.org_id()

    assert org_id == "00Dabc000000def"
    assert userinfo_route.call_count == 2


@respx.mock
async def test_org_id_userinfo_403_fails_fast_as_auth_error(
    sf_config: SalesforceConfig,
) -> None:
    respx.post(TOKEN_ENDPOINT).mock(return_value=httpx.Response(200, json=_token_response()))
    userinfo_url = f"{INSTANCE_URL}/services/oauth2/userinfo"
    userinfo_route = respx.get(userinfo_url).mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )

    async with httpx.AsyncClient() as client:
        provider = TokenProvider(sf_config, client)
        with pytest.raises(AuthError):
            await provider.org_id()

    assert userinfo_route.call_count == 1  # 4xx: no retry
