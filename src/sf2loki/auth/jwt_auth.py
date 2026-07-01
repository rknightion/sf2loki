"""Salesforce OAuth 2.0 JWT bearer flow — server-to-server token provider.

Ref: DESIGN.md §5 + §13.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import jwt
import tenacity

from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics

# JWT assertion lifetime — Salesforce rejects exp > 3 minutes in the future.
_JWT_LIFETIME: timedelta = timedelta(seconds=180)

# Proactive refresh skew: re-mint when less than this time remains on the token.
_REFRESH_SKEW: timedelta = timedelta(seconds=60)


class AuthError(Exception):
    """Raised when the Salesforce token endpoint returns a non-retryable error."""


def _should_retry(exc: BaseException) -> bool:
    """Return True only for transport errors or 5xx HTTP responses."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, _TokenEndpointError):
        return exc.status_code >= 500
    return False


class _TokenEndpointError(Exception):
    """Internal: wraps an HTTP error response from a Salesforce OAuth endpoint."""

    def __init__(self, status_code: int, body: str, what: str = "token endpoint") -> None:
        super().__init__(f"{what} HTTP {status_code}: {body}")
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class AccessToken:
    """A Salesforce access token with its associated metadata."""

    value: str
    instance_url: str
    expires_at: datetime


class TokenProvider:
    """Manages Salesforce access tokens via the OAuth 2.0 JWT bearer flow.

    Tokens are cached in memory; the cache is cleared either reactively via
    :meth:`invalidate` (called by callers that receive a 401) or proactively
    when the token is within :data:`_REFRESH_SKEW` of expiry.

    Thread-/task-safety: an :class:`asyncio.Lock` ensures that concurrent
    ``await token()`` calls do not double-mint.
    """

    def __init__(
        self,
        cfg: SalesforceConfig,
        client: httpx.AsyncClient,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._cached: AccessToken | None = None
        self._org_id_cached: str | None = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._metrics = metrics if metrics is not None else Metrics()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def token(self) -> AccessToken:
        """Return a valid access token, minting a new one if necessary.

        Concurrent callers share the same lock; only one mint request is
        in-flight at a time (double-checked inside the lock).
        """
        # Fast path: check outside the lock first to avoid contention when the
        # token is still valid.
        if self._is_valid(self._cached):
            return self._cached  # type: ignore[return-value]

        async with self._lock:
            # Double-check: another coroutine may have minted while we waited.
            if self._is_valid(self._cached):
                return self._cached  # type: ignore[return-value]
            self._cached = await self._mint_token()
        return self._cached

    async def org_id(self) -> str:
        """Return the Salesforce organisation ID.

        Uses :attr:`~SalesforceConfig.org_id` from config if provided;
        otherwise resolves once via the ``/services/oauth2/userinfo`` endpoint
        and caches the result.
        """
        if self._cfg.org_id is not None:
            return self._cfg.org_id

        if self._org_id_cached is not None:
            return self._org_id_cached

        tok = await self.token()
        userinfo_url = f"{tok.instance_url}/services/oauth2/userinfo"

        async def _fetch() -> str:
            response = await self._client.get(
                userinfo_url,
                headers={"Authorization": f"Bearer {tok.value}"},
            )
            if not response.is_success:
                raise _TokenEndpointError(response.status_code, response.text, "userinfo endpoint")
            organization_id: str = response.json()["organization_id"]
            return organization_id

        # Same policy as token minting: transient 5xx/transport blips at
        # startup are retried; 4xx fails fast (a network hiccup here used to
        # crash the whole process).
        try:
            async for attempt in self._retry_policy():
                with attempt:
                    self._org_id_cached = await _fetch()
                    return self._org_id_cached
        except tenacity.RetryError as exc:
            cause = exc.last_attempt.exception()
            raise AuthError(f"userinfo endpoint unreachable after retries: {cause}") from cause
        except _TokenEndpointError as exc:
            # 4xx: not retried; surface immediately.
            raise AuthError(str(exc)) from exc

        # This line is unreachable but satisfies mypy's exhaustiveness check.
        raise AuthError("userinfo fetch failed unexpectedly")  # pragma: no cover

    def invalidate(self) -> None:
        """Clear the cached token (call when a downstream caller receives a 401)."""
        self._cached = None

    def has_token(self) -> bool:
        """True if a currently-valid (non-near-expiry) token is cached.

        Used by the multi-org readiness check to tell whether an org that failed
        the startup auth probe has since recovered (its sources mint reactively).
        """
        return self._is_valid(self._cached)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_valid(token: AccessToken | None) -> bool:
        """Return True if *token* is present and not near expiry."""
        if token is None:
            return False
        return datetime.now(UTC) < token.expires_at - _REFRESH_SKEW

    def _mint_jwt(self) -> str:
        """Mint a signed RS256 JWT assertion for the token endpoint."""
        now = datetime.now(UTC)
        payload = {
            "iss": self._cfg.client_id,
            "sub": self._cfg.username,
            "aud": self._cfg.login_url,
            "exp": now + _JWT_LIFETIME,
        }
        assert self._cfg.private_key is not None, "private key must be resolved before use"
        return jwt.encode(payload, self._cfg.private_key.get_secret_value(), algorithm="RS256")

    async def _request_token(self) -> AccessToken:
        """POST to the token endpoint and parse the response.

        Raises :class:`_TokenEndpointError` on any non-2xx response so that
        tenacity can inspect the status code and decide whether to retry.
        """
        token_url = f"{self._cfg.login_url}/services/oauth2/token"
        if self._cfg.auth_mode == "client_credentials":
            assert self._cfg.client_secret is not None, "client secret must be resolved before use"
            data = {
                "grant_type": "client_credentials",
                "client_id": self._cfg.client_id,
                "client_secret": self._cfg.client_secret.get_secret_value(),
            }
        else:
            data = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": self._mint_jwt(),
            }
        response = await self._client.post(token_url, data=data)
        if not response.is_success:
            raise _TokenEndpointError(response.status_code, response.text)

        body = response.json()
        # Neither the JWT bearer nor the client_credentials response carries
        # expires_in — the real lifetime is the org's session timeout, so we
        # assume the configured salesforce.token_ttl (default 1h) and rely on
        # reactive invalidate()-on-401 for orgs with shorter timeouts.
        return AccessToken(
            value=body["access_token"],
            instance_url=body["instance_url"],
            expires_at=datetime.now(UTC) + self._cfg.token_ttl,
        )

    @staticmethod
    def _retry_policy() -> tenacity.AsyncRetrying:
        """Shared retry policy: 5xx/transport errors retried, 4xx fail fast."""
        return tenacity.AsyncRetrying(
            retry=tenacity.retry_if_exception(_should_retry),
            stop=tenacity.stop_after_attempt(4),
            wait=tenacity.wait_exponential_jitter(initial=0.5, max=10.0),
            reraise=False,
        )

    async def _mint_token(self) -> AccessToken:
        """Mint a new token with tenacity retry on 5xx / transport errors.

        A 4xx response is immediately re-raised as :class:`AuthError` without
        any retry.
        """
        try:
            async for attempt in self._retry_policy():
                with attempt:
                    token = await self._request_token()
                    self._metrics.auth_refreshes.inc()
                    return token
        except tenacity.RetryError as exc:
            self._metrics.auth_errors.inc()
            # Unwrap the underlying exception for a cleaner error message.
            cause = exc.last_attempt.exception()
            if isinstance(cause, _TokenEndpointError):
                raise AuthError(str(cause)) from cause
            raise AuthError(f"token endpoint unreachable after retries: {cause}") from cause
        except _TokenEndpointError as exc:
            self._metrics.auth_errors.inc()
            # 4xx: not retried; surface immediately.
            raise AuthError(str(exc)) from exc

        # This line is unreachable but satisfies mypy's exhaustiveness check.
        raise AuthError("token minting failed unexpectedly")  # pragma: no cover
