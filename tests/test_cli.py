"""Tests for the CLI entrypoint (cli.py), focused on --check (offline validation)."""

from __future__ import annotations

from pathlib import Path

import pytest

from sf2loki.cli import main


def _valid_config(tmp_path: Path) -> Path:
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
""".lstrip()
    )
    return p


def test_check_ok_returns_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["--config", str(_valid_config(tmp_path)), "--check"])
    assert rc == 0
    assert "OK" in capsys.readouterr().out


def test_check_fails_on_missing_secret(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(
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
    rc = main(["--config", str(p), "--check"])
    assert rc == 2
    assert "FAILED" in capsys.readouterr().err


def test_check_fails_on_source_overlap(tmp_path: Path) -> None:
    """Two sources feeding the same category must fail the offline check."""
    key = tmp_path / "key.pem"
    key.write_text("PK")
    p = tmp_path / "overlap.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sources:
  pubsub:
    enabled: true
    topics: ["/event/LoginEventStream"]
  eventlog_objects:
    enabled: true
    objects:
      - {{name: LoginEvent}}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    rc = main(["--config", str(p), "--check"])
    assert rc == 2


def test_config_example_subcommand_prints_yaml(capsys):
    rc = main(["config", "example"])
    out = capsys.readouterr().out
    assert rc == 0 and "salesforce:" in out


def test_config_schema_subcommand_prints_json(capsys):
    rc = main(["config", "schema"])
    out = capsys.readouterr().out
    assert rc == 0 and '"title": "Config"' in out


def test_version_flag_prints_version_and_exits_zero(capsys):
    # argparse's action="version" prints to stdout and raises SystemExit(0).
    from sf2loki import __version__

    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert out.startswith("sf2loki ")
    # Installed metadata should agree with the in-tree constant.
    assert __version__ in out


def test_run_path_config_error_prints_message_and_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The normal run path must not dump a traceback on bad config (D6a)."""
    p = tmp_path / "bad.yaml"
    p.write_text(
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
    rc = main(["--config", str(p)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "cannot read" in err
    assert "Traceback" not in err


def test_state_show_subcommand_dispatches_and_prints_checkpoints(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.state.file_store import FileCheckpointStore

    state_path = tmp_path / "state.json"
    p = _valid_config(tmp_path)
    p.write_text(
        p.read_text()
        + f"""
state:
  store: file
  file:
    path: {state_path}
"""
    )
    cfg = load(p)
    store = FileCheckpointStore(cfg.state.file.path)
    import asyncio

    asyncio.run(store.commit("pubsub:/event/A", "42"))
    store.close()

    rc = main(["--config", str(p), "state", "show"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "pubsub:/event/A" in out
    assert "42" in out


def test_state_show_subcommand_key_glob_arg_is_parsed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.json"
    p = _valid_config(tmp_path)
    p.write_text(
        p.read_text()
        + f"""
state:
  store: file
  file:
    path: {state_path}
"""
    )
    rc = main(["--config", str(p), "state", "show", "--key", "no-match-*"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "no checkpoints match" in out


def test_state_set_subcommand_dispatches_and_writes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from sf2loki.config import load
    from sf2loki.state.file_store import FileCheckpointStore

    state_path = tmp_path / "state.json"
    p = _valid_config(tmp_path)
    p.write_text(
        p.read_text()
        + f"""
state:
  store: file
  file:
    path: {state_path}
"""
    )
    rc = main(["--config", str(p), "state", "set", "pubsub:/event/A", "99"])
    assert rc == 0
    assert "set pubsub:/event/A = 99" in capsys.readouterr().out

    cfg = load(p)
    store = FileCheckpointStore(cfg.state.file.path, exclusive_lock=False)
    import asyncio

    assert asyncio.run(store.load("pubsub:/event/A")) == "99"


def test_state_delete_subcommand_dispatches_and_removes(tmp_path: Path) -> None:
    import asyncio

    from sf2loki.config import load
    from sf2loki.state.file_store import FileCheckpointStore

    state_path = tmp_path / "state.json"
    p = _valid_config(tmp_path)
    p.write_text(
        p.read_text()
        + f"""
state:
  store: file
  file:
    path: {state_path}
"""
    )
    cfg = load(p)
    store = FileCheckpointStore(cfg.state.file.path)
    asyncio.run(store.commit("pubsub:/event/A", "42"))
    store.close()

    rc = main(["--config", str(p), "state", "delete", "pubsub:/event/A"])
    assert rc == 0

    store2 = FileCheckpointStore(cfg.state.file.path, exclusive_lock=False)
    assert asyncio.run(store2.load("pubsub:/event/A")) is None


def test_state_subcommand_refuses_locked_file_store_unless_force(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import asyncio

    from sf2loki.config import load
    from sf2loki.state.file_store import FileCheckpointStore

    state_path = tmp_path / "state.json"
    p = _valid_config(tmp_path)
    p.write_text(
        p.read_text()
        + f"""
state:
  store: file
  file:
    path: {state_path}
"""
    )
    cfg = load(p)
    daemon_store = FileCheckpointStore(cfg.state.file.path)
    asyncio.run(daemon_store.commit("k1", "v1"))
    try:
        rc = main(["--config", str(p), "state", "show"])
        err = capsys.readouterr().err
        assert rc != 0
        assert "--force" in err

        rc_forced = main(["--config", str(p), "state", "show", "--force"])
        assert rc_forced == 0
    finally:
        daemon_store.close()


def test_state_subcommand_requires_a_sub_subcommand(tmp_path: Path) -> None:
    """`sf2loki state` with no show/set/delete must be an argparse error, not a crash."""
    with pytest.raises(SystemExit) as exc_info:
        main(["state"])
    assert exc_info.value.code != 0


def test_check_and_run_and_backfill_share_one_config_error_exit_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Prod-readiness audit #71 item 4: --check, run, and backfill must all
    report the same exit code for the same bad config (previously 1 vs 2)."""
    p = tmp_path / "bad.yaml"
    p.write_text(
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

    rc_check = main(["--config", str(p), "--check"])
    capsys.readouterr()
    rc_run = main(["--config", str(p)])
    capsys.readouterr()
    rc_backfill = main(["--config", str(p), "backfill", "--since", "2026-01-01"])
    capsys.readouterr()

    assert rc_check == rc_run == rc_backfill == 2
