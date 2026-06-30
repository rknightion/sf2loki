"""Tests for ConfigMapCheckpointStore."""

from __future__ import annotations

import httpx
import pytest
import respx

from sf2loki.state.configmap_store import ConfigMapCheckpointStore

BASE_URL = "https://kubernetes.default.svc"
CM_PATH = "/api/v1/namespaces/default/configmaps/sf2loki-state"
TOKEN = "test-token"


def _make_store() -> ConfigMapCheckpointStore:
    client = httpx.AsyncClient(base_url=BASE_URL)
    return ConfigMapCheckpointStore(
        name="sf2loki-state",
        namespace="default",
        token=TOKEN,
        client=client,
    )


def _cm_response(data: dict[str, str], resource_version: str = "123") -> dict:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "sf2loki-state",
            "namespace": "default",
            "resourceVersion": resource_version,
        },
        "data": data,
    }


@pytest.mark.asyncio
async def test_load_returns_value_from_configmap() -> None:
    with respx.mock(base_url=BASE_URL) as mock:
        mock.get(CM_PATH).mock(
            return_value=httpx.Response(200, json=_cm_response({"stream-a": "offset-10"}))
        )
        store = _make_store()
        result = await store.load("stream-a")
        assert result == "offset-10"


@pytest.mark.asyncio
async def test_load_missing_key_returns_none() -> None:
    with respx.mock(base_url=BASE_URL) as mock:
        mock.get(CM_PATH).mock(
            return_value=httpx.Response(200, json=_cm_response({"other-key": "x"}))
        )
        store = _make_store()
        result = await store.load("stream-a")
        assert result is None


@pytest.mark.asyncio
async def test_load_404_returns_none() -> None:
    with respx.mock(base_url=BASE_URL) as mock:
        mock.get(CM_PATH).mock(return_value=httpx.Response(404, json={"kind": "Status"}))
        store = _make_store()
        result = await store.load("stream-a")
        assert result is None


@pytest.mark.asyncio
async def test_commit_gets_then_puts_with_resource_version() -> None:
    with respx.mock(base_url=BASE_URL) as mock:
        get_route = mock.get(CM_PATH).mock(
            return_value=httpx.Response(200, json=_cm_response({}, resource_version="99"))
        )
        put_route = mock.put(CM_PATH).mock(
            return_value=httpx.Response(
                200, json=_cm_response({"stream-a": "offset-5"}, resource_version="100")
            )
        )

        store = _make_store()
        await store.commit("stream-a", "offset-5")

        assert get_route.called
        assert put_route.called

        put_request = put_route.calls.last.request
        import json

        body = json.loads(put_request.content)
        assert body["metadata"]["resourceVersion"] == "99"
        assert body["data"]["stream-a"] == "offset-5"


@pytest.mark.asyncio
async def test_commit_retries_on_409() -> None:
    """On 409, store must re-GET (fresh resourceVersion) and retry the PUT."""
    with respx.mock(base_url=BASE_URL) as mock:
        # Two GETs: first for original rv, second for fresh rv after conflict
        get_responses = [
            httpx.Response(200, json=_cm_response({}, resource_version="10")),
            httpx.Response(200, json=_cm_response({}, resource_version="11")),
        ]
        mock.get(CM_PATH).mock(side_effect=get_responses)

        # Two PUTs: first conflicts, second succeeds
        put_responses = [
            httpx.Response(409, json={"kind": "Status", "reason": "Conflict"}),
            httpx.Response(
                200, json=_cm_response({"stream-a": "offset-99"}, resource_version="12")
            ),
        ]
        mock.put(CM_PATH).mock(side_effect=put_responses)

        store = _make_store()
        await store.commit("stream-a", "offset-99")

        # Both PUTs were called, meaning it retried
        assert mock.put(CM_PATH).call_count == 2
        # Both GETs were called (re-fetch on conflict)
        assert mock.get(CM_PATH).call_count == 2


@pytest.mark.asyncio
async def test_commit_auth_header_sent() -> None:
    """Every request must carry the Bearer token."""
    with respx.mock(base_url=BASE_URL) as mock:
        get_route = mock.get(CM_PATH).mock(return_value=httpx.Response(200, json=_cm_response({})))
        mock.put(CM_PATH).mock(return_value=httpx.Response(200, json=_cm_response({"k": "v"})))

        store = _make_store()
        await store.commit("k", "v")

        get_request = get_route.calls.last.request
        assert get_request.headers["authorization"] == f"Bearer {TOKEN}"
