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

# JWT assertion lifetime — Salesforce rejects exp > 3 minutes in the future.
_JWT_LIFETIME: timedelta = timedelta(seconds=180)

# Conservative access-token TTL — the JWT flow returns no expires_in, so we
# choose a safe 1-hour window.  Refresh is primarily reactive (invalidate() on
# a downstream 401) plus proactive when within _REFRESH_SKEW of expiry.
TOKEN_TTL: timedelta = timedelta(hours=1)

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
    """Internal: wraps an HTTP error response from the token endpoint."""

    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"token endpoint HTTP {status_code}: {body}")
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

    def __init__(self, cfg: SalesforceConfig, client: httpx.AsyncClient) -> None:
        self._cfg = cfg
        self._client = client
        self._cached: AccessToken | None = None
        self._org_id_cached: str | None = None
        self._lock: asyncio.Lock = asyncio.Lock()

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
        response = await self._client.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {tok.value}"},
        )
        response.raise_for_status()
        self._org_id_cached = response.json()["organization_id"]
        return self._org_id_cached

    def invalidate(self) -> None:
        """Clear the cached token (call when a downstream caller receives a 401)."""
        self._cached = None

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
        assertion = self._mint_jwt()
        token_url = f"{self._cfg.login_url}/services/oauth2/token"
        response = await self._client.post(
            token_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            },
        )
        if not response.is_success:
            raise _TokenEndpointError(response.status_code, response.text)

        body = response.json()
        return AccessToken(
            value=body["access_token"],
            instance_url=body["instance_url"],
            expires_at=datetime.now(UTC) + TOKEN_TTL,
        )

    async def _mint_token(self) -> AccessToken:
        """Mint a new token with tenacity retry on 5xx / transport errors.

        A 4xx response is immediately re-raised as :class:`AuthError` without
        any retry.
        """
        retry = tenacity.AsyncRetrying(
            retry=tenacity.retry_if_exception(_should_retry),
            stop=tenacity.stop_after_attempt(4),
            wait=tenacity.wait_exponential_jitter(initial=0.5, max=10.0),
            reraise=False,
        )
        try:
            async for attempt in retry:
                with attempt:
                    return await self._request_token()
        except tenacity.RetryError as exc:
            # Unwrap the underlying exception for a cleaner error message.
            cause = exc.last_attempt.exception()
            if isinstance(cause, _TokenEndpointError):
                raise AuthError(str(cause)) from cause
            raise AuthError(f"token endpoint unreachable after retries: {cause}") from cause
        except _TokenEndpointError as exc:
            # 4xx: not retried; surface immediately.
            raise AuthError(str(exc)) from exc

        # This line is unreachable but satisfies mypy's exhaustiveness check.
        raise AuthError("token minting failed unexpectedly")  # pragma: no cover
