"""Tests for `sf2loki state` (issue #63): checkpoint inspect/repair.

Three layers:
1. The new `delete` method on each of the three CheckpointStore backends
   (file/s3/gcs) — CAS/fence discipline mirroring `commit`/`commit_many`.
2. `statecmd.run_state_show/set/delete` — config loading, the file store's
   lock-refusal-unless-`--force`, and object-store conflict-safety, with
   `build_store` monkeypatched (mirrors tests/test_doctor.py's pattern) so
   s3/gcs paths run without the optional extras installed.
3. `cli.main`'s `state` subcommand argument parsing/dispatch, in
   tests/test_cli.py (not here).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from sf2loki.config import GcsStateConfig, S3StateConfig
from sf2loki.state.file_store import FileCheckpointStore
from sf2loki.state.gcs_store import GcsCheckpointStore
from sf2loki.state.s3_store import S3CheckpointStore, StateStoreConflictError

# ---------------------------------------------------------------------------
# Layer 1a: FileCheckpointStore.delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_store_delete_removes_existing_key(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "v1")
    await store.delete("k1")
    assert await store.load("k1") is None


@pytest.mark.asyncio
async def test_file_store_delete_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store1 = FileCheckpointStore(path)
    await store1.commit("k1", "v1")
    await store1.commit("k2", "v2")
    await store1.delete("k1")
    store1.close()

    store2 = FileCheckpointStore(path)
    assert await store2.load("k1") is None
    assert await store2.load("k2") == "v2"


@pytest.mark.asyncio
async def test_file_store_delete_missing_key_is_noop(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "v1")
    await store.delete("no-such-key")  # must not raise
    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_file_store_delete_respects_fence(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "v1")

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.delete("k1")
    assert await store.load("k1") == "v1"  # untouched


@pytest.mark.asyncio
async def test_file_store_delete_epoch_fenced_rejects_stale_epoch(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    leader1 = FileCheckpointStore(path)
    leader1.set_epoch(lambda: 1)
    await leader1.commit("k1", "v1")

    leader2 = FileCheckpointStore(path, exclusive_lock=False)
    leader2.set_epoch(lambda: 2)
    await leader2.commit("k2", "v2")  # advances the persisted epoch to 2

    from sf2loki.coordinate.base import StateFenceError

    with pytest.raises(StateFenceError):
        await leader1.delete("k1")  # leader1's epoch (1) is now stale


@pytest.mark.asyncio
async def test_file_store_delete_epoch_fenced_removes_key(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    store.set_epoch(lambda: 5)
    await store.commit("k1", "v1")
    await store.delete("k1")
    assert await store.load("k1") is None


# ---------------------------------------------------------------------------
# Layer 1b: S3CheckpointStore.delete (minimal in-memory fake client)
# ---------------------------------------------------------------------------


class _FakeS3ClientError(Exception):
    def __init__(self, code: str, status: int) -> None:
        self.response = {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }
        super().__init__(code)


class _FakeStreamingBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self) -> bytes:
        return self._data


class _FakeS3Backend:
    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}
        self._etag_counter = 0
        self.put_object_calls = 0

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        obj = self._objects.get(Key)
        if obj is None:
            raise _FakeS3ClientError("NoSuchKey", 404)
        body, etag = obj
        return {"Body": _FakeStreamingBody(body), "ETag": etag}

    async def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, Any]:
        self.put_object_calls += 1
        existing = self._objects.get(Key)
        if IfNoneMatch == "*":
            if existing is not None:
                raise _FakeS3ClientError("PreconditionFailed", 412)
        elif IfMatch is not None and (existing is None or existing[1] != IfMatch):
            raise _FakeS3ClientError("PreconditionFailed", 412)
        self._etag_counter += 1
        etag = f"etag-{self._etag_counter}"
        self._objects[Key] = (Body, etag)
        return {"ETag": etag}


def _s3_store(backend: _FakeS3Backend) -> S3CheckpointStore:
    @asynccontextmanager
    async def factory():  # type: ignore[no-untyped-def]
        yield backend

    cfg = S3StateConfig(bucket="b", key="sf2loki/state.json")
    return S3CheckpointStore(cfg, client_factory=factory)


@pytest.mark.asyncio
async def test_s3_store_delete_removes_key_via_conditional_put() -> None:
    backend = _FakeS3Backend()
    store = _s3_store(backend)
    await store.commit("k1", "v1")
    await store.commit("k2", "v2")

    await store.delete("k1")

    assert await store.load("k1") is None
    assert await store.load("k2") == "v2"


@pytest.mark.asyncio
async def test_s3_store_delete_missing_key_is_noop_no_put() -> None:
    backend = _FakeS3Backend()
    store = _s3_store(backend)
    await store.commit("k1", "v1")
    calls_before = backend.put_object_calls

    await store.delete("no-such-key")

    assert backend.put_object_calls == calls_before
    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_s3_store_delete_conflict_raises_state_store_conflict_error() -> None:
    """Two stores race: one commits, the other's delete loses the CAS."""
    backend = _FakeS3Backend()
    store1 = _s3_store(backend)
    store2 = _s3_store(backend)

    await store1.commit("k1", "v1")
    await store2.load("k1")  # store2 caches the pre-race etag

    await store1.commit("k1", "v2")  # store1 advances the object
    with pytest.raises(StateStoreConflictError):
        await store2.delete("k1")  # store2's stale etag is rejected


@pytest.mark.asyncio
async def test_s3_store_delete_respects_fence() -> None:
    backend = _FakeS3Backend()
    store = _s3_store(backend)
    await store.commit("k1", "v1")

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.delete("k1")
    assert await store.load("k1") == "v1"


# ---------------------------------------------------------------------------
# Layer 1c: GcsCheckpointStore.delete (minimal in-memory fake client)
# ---------------------------------------------------------------------------


class _FakeGcsError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(status)


class _FakeGcsBackend:
    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, int]] = {}
        self._generation_counter = 0
        self.upload_calls = 0

    async def download(self, bucket: str, object_name: str) -> bytes:
        obj = self._objects.get(object_name)
        if obj is None:
            raise _FakeGcsError(404)
        return obj[0]

    async def download_metadata(self, bucket: str, object_name: str) -> dict[str, Any]:
        obj = self._objects.get(object_name)
        if obj is None:
            raise _FakeGcsError(404)
        return {"generation": obj[1]}

    async def upload(
        self, bucket: str, object_name: str, data: bytes, *, parameters: dict[str, str]
    ) -> dict[str, Any]:
        self.upload_calls += 1
        existing = self._objects.get(object_name)
        match = parameters.get("ifGenerationMatch")
        if match == "0":
            if existing is not None:
                raise _FakeGcsError(412)
        elif match is not None and (existing is None or str(existing[1]) != match):
            raise _FakeGcsError(412)
        self._generation_counter += 1
        generation = self._generation_counter
        self._objects[object_name] = (data, generation)
        return {"generation": generation}


def _gcs_store(backend: _FakeGcsBackend) -> GcsCheckpointStore:
    @asynccontextmanager
    async def factory():  # type: ignore[no-untyped-def]
        yield backend

    cfg = GcsStateConfig(bucket="b", object_name="sf2loki/state.json")
    return GcsCheckpointStore(cfg, client_factory=factory)


@pytest.mark.asyncio
async def test_gcs_store_delete_removes_key_via_conditional_upload() -> None:
    backend = _FakeGcsBackend()
    store = _gcs_store(backend)
    await store.commit("k1", "v1")
    await store.commit("k2", "v2")

    await store.delete("k1")

    assert await store.load("k1") is None
    assert await store.load("k2") == "v2"


@pytest.mark.asyncio
async def test_gcs_store_delete_missing_key_is_noop_no_upload() -> None:
    backend = _FakeGcsBackend()
    store = _gcs_store(backend)
    await store.commit("k1", "v1")
    calls_before = backend.upload_calls

    await store.delete("no-such-key")

    assert backend.upload_calls == calls_before
    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_gcs_store_delete_conflict_raises_state_store_conflict_error() -> None:
    backend = _FakeGcsBackend()
    store1 = _gcs_store(backend)
    store2 = _gcs_store(backend)

    await store1.commit("k1", "v1")
    await store2.load("k1")  # store2 caches the pre-race generation

    await store1.commit("k1", "v2")  # store1 advances the object
    with pytest.raises(StateStoreConflictError):
        await store2.delete("k1")  # store2's stale generation is rejected


@pytest.mark.asyncio
async def test_gcs_store_delete_respects_fence() -> None:
    backend = _FakeGcsBackend()
    store = _gcs_store(backend)
    await store.commit("k1", "v1")

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.delete("k1")
    assert await store.load("k1") == "v1"


# ---------------------------------------------------------------------------
# Layer 2: statecmd.run_state_show/set/delete
# ---------------------------------------------------------------------------


def _config_yaml(tmp_path: Path, state_yaml: str = "") -> Path:
    key = tmp_path / "key.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sources:
  pubsub:
    enabled: false
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
{state_yaml}
""".lstrip()
    )
    return p


def _file_config(tmp_path: Path) -> Path:
    state_path = tmp_path / "state.json"
    return _config_yaml(
        tmp_path,
        f"""
state:
  store: file
  file:
    path: {state_path}
""",
    )


@pytest.mark.asyncio
async def test_run_state_show_lists_all_keys_by_default(tmp_path: Path) -> None:
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    from sf2loki.config import load

    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path)
    await store.commit_many({"pubsub:/event/A": "1", "eventlog_objects:LoginEvent": "2"})
    store.close()

    rc = await run_state_show(cfg_path)
    assert rc == 0


@pytest.mark.asyncio
async def test_run_state_show_filters_by_glob(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path)
    await store.commit_many({"pubsub:/event/A": "1", "eventlog_objects:LoginEvent": "2"})
    store.close()

    rc = await run_state_show(cfg_path, key_glob="pubsub:*")
    out = capsys.readouterr().out
    assert rc == 0
    assert "pubsub:/event/A" in out
    assert "eventlog_objects:LoginEvent" not in out


@pytest.mark.asyncio
async def test_run_state_show_does_not_redact_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path)
    await store.commit("pubsub:/event/A", "replay-id-abc123")
    store.close()

    rc = await run_state_show(cfg_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "replay-id-abc123" in out


@pytest.mark.asyncio
async def test_run_state_show_hides_reserved_epoch_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path)
    store.set_epoch(lambda: 1)
    await store.commit("k1", "v1")
    store.close()

    rc = await run_state_show(cfg_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "__fence_epoch__" not in out
    assert "k1" in out


@pytest.mark.asyncio
async def test_run_state_set_writes_key(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from sf2loki.config import load
    from sf2loki.statecmd import run_state_set

    cfg_path = _file_config(tmp_path)
    rc = await run_state_set(cfg_path, "pubsub:/event/A", "42")
    assert rc == 0
    assert "set pubsub:/event/A = 42" in capsys.readouterr().out

    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path, exclusive_lock=False)
    assert await store.load("pubsub:/event/A") == "42"


@pytest.mark.asyncio
async def test_run_state_set_refuses_reserved_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.statecmd import run_state_set

    cfg_path = _file_config(tmp_path)
    rc = await run_state_set(cfg_path, "__fence_epoch__", "99")
    assert rc != 0
    assert "reserved" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_run_state_delete_removes_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.statecmd import run_state_delete

    cfg_path = _file_config(tmp_path)
    cfg = load(cfg_path)
    store = FileCheckpointStore(cfg.state.file.path)
    await store.commit("pubsub:/event/A", "42")
    store.close()

    rc = await run_state_delete(cfg_path, "pubsub:/event/A")
    assert rc == 0
    assert "deleted pubsub:/event/A" in capsys.readouterr().out

    store2 = FileCheckpointStore(cfg.state.file.path, exclusive_lock=False)
    assert await store2.load("pubsub:/event/A") is None


@pytest.mark.asyncio
async def test_run_state_delete_missing_key_is_noop_and_still_exits_zero(
    tmp_path: Path,
) -> None:
    from sf2loki.statecmd import run_state_delete

    cfg_path = _file_config(tmp_path)
    rc = await run_state_delete(cfg_path, "no-such-key")
    assert rc == 0


@pytest.mark.asyncio
async def test_run_state_delete_refuses_reserved_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.statecmd import run_state_delete

    cfg_path = _file_config(tmp_path)
    rc = await run_state_delete(cfg_path, "__fence_epoch__")
    assert rc != 0
    assert "reserved" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_run_state_show_config_error_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.statecmd import run_state_show

    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: /does/not/exist.pem
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    rc = await run_state_show(bad)
    assert rc == 2
    assert "sf2loki:" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Layer 2b: file-store lock refusal (the daemon is running) unless --force.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_state_show_refuses_when_daemon_holds_the_lock(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    from sf2loki.config import load

    cfg = load(cfg_path)
    daemon_store = FileCheckpointStore(cfg.state.file.path)
    await daemon_store.commit("k1", "v1")  # acquires + holds the exclusive lock
    try:
        rc = await run_state_show(cfg_path)
        err = capsys.readouterr().err
        assert rc != 0
        assert "--force" in err
    finally:
        daemon_store.close()


@pytest.mark.asyncio
async def test_run_state_show_force_bypasses_the_lock(tmp_path: Path) -> None:
    from sf2loki.statecmd import run_state_show

    cfg_path = _file_config(tmp_path)
    from sf2loki.config import load

    cfg = load(cfg_path)
    daemon_store = FileCheckpointStore(cfg.state.file.path)
    await daemon_store.commit("k1", "v1")
    try:
        rc = await run_state_show(cfg_path, force=True)
        assert rc == 0
    finally:
        daemon_store.close()


@pytest.mark.asyncio
async def test_run_state_set_refuses_when_daemon_holds_the_lock(tmp_path: Path) -> None:
    from sf2loki.statecmd import run_state_set

    cfg_path = _file_config(tmp_path)
    from sf2loki.config import load

    cfg = load(cfg_path)
    daemon_store = FileCheckpointStore(cfg.state.file.path)
    await daemon_store.commit("k1", "v1")
    try:
        rc = await run_state_set(cfg_path, "k2", "v2")
        assert rc != 0
    finally:
        daemon_store.close()


@pytest.mark.asyncio
async def test_run_state_delete_refuses_when_daemon_holds_the_lock(tmp_path: Path) -> None:
    from sf2loki.statecmd import run_state_delete

    cfg_path = _file_config(tmp_path)
    from sf2loki.config import load

    cfg = load(cfg_path)
    daemon_store = FileCheckpointStore(cfg.state.file.path)
    await daemon_store.commit("k1", "v1")
    try:
        rc = await run_state_delete(cfg_path, "k1")
        assert rc != 0
    finally:
        daemon_store.close()


# ---------------------------------------------------------------------------
# Layer 2c: object-store conflict-safety surfaced through statecmd, with
# build_store monkeypatched (mirrors tests/test_doctor.py's pattern) so this
# runs without the sf2loki[s3]/[gcs] extras installed.
# ---------------------------------------------------------------------------


def _s3_config_yaml(tmp_path: Path) -> Path:
    return _config_yaml(
        tmp_path,
        """
state:
  store: s3
  s3:
    bucket: test-bucket
    key: sf2loki/state.json
""",
    )


@pytest.mark.asyncio
async def test_run_state_set_surfaces_s3_conflict_as_clear_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sf2loki.statecmd as statecmd_module

    backend = _FakeS3Backend()
    # Pre-populate so the store's cached etag goes stale the moment another
    # writer commits underneath it.
    seed = _s3_store(backend)
    await seed.load("k1")  # seed observes the (absent) object

    racer = _s3_store(backend)
    await racer.commit("k1", "raced-in")  # a concurrent writer wins first

    monkeypatch.setattr(statecmd_module, "build_store", lambda cfg, **kw: seed)

    rc = await statecmd_module.run_state_set(_s3_config_yaml(tmp_path), "k1", "mine")
    err = capsys.readouterr().err
    assert rc != 0
    assert "retry" in err.lower()


@pytest.mark.asyncio
async def test_run_state_delete_surfaces_s3_conflict_as_clear_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sf2loki.statecmd as statecmd_module

    backend = _FakeS3Backend()
    seed = _s3_store(backend)
    await seed.commit("k1", "v1")  # seed's cached etag is now the pre-race one

    racer = _s3_store(backend)
    await racer.commit("k1", "raced-in")

    monkeypatch.setattr(statecmd_module, "build_store", lambda cfg, **kw: seed)

    rc = await statecmd_module.run_state_delete(_s3_config_yaml(tmp_path), "k1")
    err = capsys.readouterr().err
    assert rc != 0
    assert "retry" in err.lower()


@pytest.mark.asyncio
async def test_run_state_show_works_against_s3_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sf2loki.statecmd as statecmd_module

    backend = _FakeS3Backend()
    store = _s3_store(backend)
    await store.commit_many({"pubsub:/event/A": "1", "pubsub:/event/B": "2"})

    monkeypatch.setattr(statecmd_module, "build_store", lambda cfg, **kw: store)

    rc = await statecmd_module.run_state_show(_s3_config_yaml(tmp_path))
    out = capsys.readouterr().out
    assert rc == 0
    assert "pubsub:/event/A" in out
    assert "pubsub:/event/B" in out


def _gcs_config_yaml(tmp_path: Path) -> Path:
    return _config_yaml(
        tmp_path,
        """
state:
  store: gcs
  gcs:
    bucket: test-bucket
    object_name: sf2loki/state.json
""",
    )


@pytest.mark.asyncio
async def test_run_state_delete_surfaces_gcs_conflict_as_clear_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sf2loki.statecmd as statecmd_module

    backend = _FakeGcsBackend()
    seed = _gcs_store(backend)
    await seed.commit("k1", "v1")

    racer = _gcs_store(backend)
    await racer.commit("k1", "raced-in")

    monkeypatch.setattr(statecmd_module, "build_store", lambda cfg, **kw: seed)

    rc = await statecmd_module.run_state_delete(_gcs_config_yaml(tmp_path), "k1")
    err = capsys.readouterr().err
    assert rc != 0
    assert "retry" in err.lower()


@pytest.mark.asyncio
async def test_run_state_show_works_against_gcs_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sf2loki.statecmd as statecmd_module

    backend = _FakeGcsBackend()
    store = _gcs_store(backend)
    await store.commit_many({"pubsub:/event/A": "1"})

    monkeypatch.setattr(statecmd_module, "build_store", lambda cfg, **kw: store)

    rc = await statecmd_module.run_state_show(_gcs_config_yaml(tmp_path))
    out = capsys.readouterr().out
    assert rc == 0
    assert "pubsub:/event/A" in out
