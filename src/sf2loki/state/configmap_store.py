"""ConfigMapCheckpointStore: durable state backed by a Kubernetes ConfigMap.

Uses the raw k8s REST API via httpx (no k8s SDK dependency).  Optimistic
concurrency is handled with resourceVersion + PUT; 409 Conflict triggers a
retry via tenacity (re-GET fresh version, then re-PUT).
"""

from __future__ import annotations

from pathlib import Path

import httpx
import tenacity

# ---------------------------------------------------------------------------
# Exceptions


class ConfigMapError(Exception):
    """Raised for unexpected errors communicating with the Kubernetes API."""


class _ConflictError(Exception):
    """Internal sentinel: PUT returned 409 Conflict — tenacity retries on this."""


# ---------------------------------------------------------------------------
# Store


class ConfigMapCheckpointStore:
    """Checkpoint store backed by a Kubernetes ConfigMap.

    The ConfigMap is expected to exist before this store is used.  Only the
    ``data`` field is managed; all other fields are preserved on each PUT.

    Args:
        name:       ConfigMap name.
        namespace:  Kubernetes namespace.
        token:      Service-account bearer token.
        client:     Pre-configured httpx.AsyncClient (base_url already set).
                    Tests inject a respx-mocked client here.
    """

    def __init__(
        self,
        *,
        name: str,
        namespace: str,
        token: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._name = name
        self._namespace = namespace
        self._token = token
        self._client = client
        self._cm_path = f"/api/v1/namespaces/{namespace}/configmaps/{name}"

    # ------------------------------------------------------------------
    # Factory for production use (reads SA files; NOT used in tests)

    @classmethod
    def from_service_account(
        cls,
        name: str,
        namespace: str | None = None,
        sa_dir: Path = Path("/var/run/secrets/kubernetes.io/serviceaccount"),
    ) -> ConfigMapCheckpointStore:
        """Build a store from the in-cluster service-account projection.

        Reads token, namespace (if not provided), and CA bundle from the
        standard SA directory.  Not tested in unit tests — network-dependent.
        """
        token = (sa_dir / "token").read_text().strip()
        if namespace is None:
            namespace = (sa_dir / "namespace").read_text().strip()
        ca_cert = str(sa_dir / "ca.crt")
        client = httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            verify=ca_cert,
        )
        return cls(name=name, namespace=namespace, token=token, client=client)

    # ------------------------------------------------------------------
    # Internal helpers

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def _get_cm(self) -> tuple[dict[str, str], str | None]:
        """GET the ConfigMap.  Returns (data_dict, resourceVersion).

        Returns ({}, None) on 404.  Raises ConfigMapError on other errors.
        """
        resp = await self._client.get(self._cm_path, headers=self._auth_headers())
        if resp.status_code == 404:
            return {}, None
        if resp.status_code != 200:
            raise ConfigMapError(
                f"GET configmap {self._name} returned {resp.status_code}: {resp.text}"
            )
        body = resp.json()
        data: dict[str, str] = body.get("data") or {}
        rv: str | None = body.get("metadata", {}).get("resourceVersion")
        return data, rv

    async def _put_cm(self, data: dict[str, str], resource_version: str | None) -> None:
        """PUT the ConfigMap with updated data.

        Raises _ConflictError on 409 (tenacity retries).
        Raises ConfigMapError on other non-2xx responses.
        """
        metadata: dict[str, str] = {
            "name": self._name,
            "namespace": self._namespace,
        }
        if resource_version is not None:
            metadata["resourceVersion"] = resource_version
        body: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "data": data,
        }

        resp = await self._client.put(
            self._cm_path,
            json=body,
            headers=self._auth_headers(),
        )
        if resp.status_code == 409:
            raise _ConflictError("resourceVersion conflict — will retry")
        if not (200 <= resp.status_code < 300):
            raise ConfigMapError(
                f"PUT configmap {self._name} returned {resp.status_code}: {resp.text}"
            )

    # ------------------------------------------------------------------
    # CheckpointStore protocol

    async def load(self, key: str) -> str | None:
        data, _ = await self._get_cm()
        return data.get(key)

    async def commit(self, key: str, value: str) -> None:
        """Read-Modify-Write with optimistic concurrency, retried on 409."""

        @tenacity.retry(
            retry=tenacity.retry_if_exception_type(_ConflictError),
            stop=tenacity.stop_after_attempt(5),
            wait=tenacity.wait_exponential(multiplier=0.1, min=0.1, max=1.0),
            reraise=True,
        )
        async def _attempt() -> None:
            data, rv = await self._get_cm()
            if rv is None:
                # ConfigMap does not exist — we require it to exist
                raise ConfigMapError(
                    f"ConfigMap {self._namespace}/{self._name} not found; "
                    "create it before using ConfigMapCheckpointStore"
                )
            data[key] = value
            await self._put_cm(data, rv)

        await _attempt()
