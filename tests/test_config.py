"""
Tests for MeshWeaver configuration loading, environment variables,
priority hierarchy, validation, and CLI startup error handling.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from unittest.mock import patch
import pytest

from meshweaver import ConfigurationError
from meshweaver.__main__ import main
from meshweaver.config import (
    NodeConfig,
    load_config_file,
    load_env_config,
    parse_env_bool,
    resolve_node_config,
    validate_node_config,
)
from meshweaver.security import SecurityConfig, TLSConfig


# =============================================================================
# 1. Config File Loading Tests
# =============================================================================


def test_load_config_file_valid(tmp_path: Path) -> None:
    """Loading a valid JSON config file returns the expected dictionary."""
    conf_file = tmp_path / "config.json"
    conf_file.write_text(
        json.dumps({
            "network": {"host": "192.168.1.10", "port": 9000},
            "seeds": ["192.168.1.1:8000"],
            "logging": {"verbose": True},
        }),
        encoding="utf-8",
    )

    data = load_config_file(conf_file)
    assert data["network"]["host"] == "192.168.1.10"
    assert data["network"]["port"] == 9000
    assert data["seeds"] == ["192.168.1.1:8000"]
    assert data["logging"]["verbose"] is True


def test_load_config_file_missing(tmp_path: Path) -> None:
    """Loading a non-existent file raises ConfigurationError with path info."""
    missing_file = tmp_path / "does_not_exist.json"
    with pytest.raises(ConfigurationError) as exc_info:
        load_config_file(missing_file)
    assert "Configuration file not found" in str(exc_info.value)


def test_load_config_file_invalid_json(tmp_path: Path) -> None:
    """Loading malformed JSON raises ConfigurationError with line and column info."""
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{ host: invalid, }", encoding="utf-8")
    with pytest.raises(ConfigurationError) as exc_info:
        load_config_file(bad_file)
    assert "Failed to parse JSON" in str(exc_info.value)


def test_load_config_file_non_dict(tmp_path: Path) -> None:
    """Loading JSON that contains a list or scalar raises ConfigurationError."""
    list_file = tmp_path / "list.json"
    list_file.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ConfigurationError) as exc_info:
        load_config_file(list_file)
    assert "must contain a top-level JSON object" in str(exc_info.value)


def test_load_example_config_file() -> None:
    """Verify that the repository's meshweaver.example.json is valid and resolvable."""
    example_path = Path("meshweaver.example.json").resolve()
    assert example_path.exists(), "meshweaver.example.json must exist in project root"

    cfg = resolve_node_config(config_path=example_path)
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8000
    assert cfg.peer_id == "mesh-node-01"
    assert len(cfg.seeds) == 2
    assert cfg.timeout == 5.0
    assert cfg.heartbeat_interval == 1.0
    assert cfg.missed_threshold == 3
    assert cfg.gossip_interval == 5.0


# =============================================================================
# 2. Environment Variable Resolution Tests
# =============================================================================


def test_parse_env_bool() -> None:
    """Test boolean parsing from environment string representations."""
    assert parse_env_bool("1") is True
    assert parse_env_bool("true") is True
    assert parse_env_bool("TRUE") is True
    assert parse_env_bool("yes") is True
    assert parse_env_bool("on") is True
    assert parse_env_bool("enabled") is True

    assert parse_env_bool("0") is False
    assert parse_env_bool("false") is False
    assert parse_env_bool("no") is False
    assert parse_env_bool("off") is False
    assert parse_env_bool("disabled") is False

    assert parse_env_bool(None, default=True) is True
    assert parse_env_bool(None, default=False) is False
    assert parse_env_bool("random", default=False) is False


def test_load_env_config_defaults() -> None:
    """Empty environment returns an empty configuration dictionary."""
    cfg = load_env_config({})
    assert cfg == {}


def test_load_env_config_all_keys() -> None:
    """Extract settings from MESHWEAVER_* environment variables."""
    fake_env = {
        "MESHWEAVER_HOST": "10.0.0.5",
        "MESHWEAVER_PORT": "8888",
        "MESHWEAVER_PEER_ID": "worker-env-1",
        "MESHWEAVER_SEEDS": "10.0.0.1:8000, 10.0.0.2:8000",
        "MESHWEAVER_TIMEOUT": "7.5",
        "MESHWEAVER_HEARTBEAT_INTERVAL": "2.0",
        "MESHWEAVER_MISSED_THRESHOLD": "5",
        "MESHWEAVER_GOSSIP_INTERVAL": "10.0",
        "MESHWEAVER_VERBOSE": "true",
        "MESHWEAVER_JSON": "1",
        "MESHWEAVER_PLAIN": "yes",
        "MESHWEAVER_TLS_ENABLED": "1",
        "MESHWEAVER_SHARED_SECRET": "test-secret-123",
        "MESHWEAVER_AUTH_TOKEN": "token-xyz",
        "MESHWEAVER_ALLOWED_PEERS": "peer-a, peer-b",
    }
    cfg = load_env_config(fake_env)

    assert cfg["host"] == "10.0.0.5"
    assert cfg["port"] == 8888
    assert cfg["peer_id"] == "worker-env-1"
    assert cfg["seeds"] == ["10.0.0.1:8000", "10.0.0.2:8000"]
    assert cfg["timeout"] == 7.5
    assert cfg["heartbeat_interval"] == 2.0
    assert cfg["missed_threshold"] == 5
    assert cfg["gossip_interval"] == 10.0
    assert cfg["verbose"] is True
    assert cfg["as_json"] is True
    assert cfg["force_plain"] is True
    assert cfg["security"]["tls"]["enabled"] is True
    assert cfg["security"]["shared_secret"] == "test-secret-123"
    assert cfg["security"]["auth_token"] == "token-xyz"
    assert cfg["security"]["allowed_peers"] == {"peer-a", "peer-b"}


def test_load_env_config_invalid_port() -> None:
    """Non-integer MESHWEAVER_PORT raises ConfigurationError."""
    with pytest.raises(ConfigurationError) as exc_info:
        load_env_config({"MESHWEAVER_PORT": "invalid_port"})
    assert "MESHWEAVER_PORT must be an integer" in str(exc_info.value)


def test_load_env_config_invalid_timeout() -> None:
    """Non-numeric MESHWEAVER_TIMEOUT raises ConfigurationError."""
    with pytest.raises(ConfigurationError) as exc_info:
        load_env_config({"MESHWEAVER_TIMEOUT": "not_a_float"})
    assert "MESHWEAVER_TIMEOUT must be a float" in str(exc_info.value)


# =============================================================================
# 3. Priority Hierarchy Tests: CLI > ENV > File > Defaults
# =============================================================================


def test_priority_hierarchy_cli_over_env_over_file_over_defaults(tmp_path: Path) -> None:
    """
    Verify strict precedence:
    CLI argument > Environment variable > Config file > Built-in default.
    """
    conf_file = tmp_path / "test_conf.json"
    conf_file.write_text(
        json.dumps({
            "network": {
                "host": "file-host",
                "port": 5000,
                "peer_id": "file-id",
                "connection_timeout": 10.0,
            },
            "seeds": ["1.1.1.1:1000"],
            "logging": {"verbose": False},
        }),
        encoding="utf-8",
    )

    env_overrides = {
        "MESHWEAVER_HOST": "env-host",
        "MESHWEAVER_PORT": "6000",
        "MESHWEAVER_PEER_ID": "env-id",
        # Notice MESHWEAVER_TIMEOUT is not set in env
    }

    # Case 1: File only -> values come from file
    cfg1 = resolve_node_config(config_path=conf_file, env_override={})
    assert cfg1.host == "file-host"
    assert cfg1.port == 5000
    assert cfg1.peer_id == "file-id"
    assert cfg1.timeout == 10.0
    assert cfg1.seeds == ["1.1.1.1:1000"]

    # Case 2: File + Env -> Env overrides file for host, port, peer_id; timeout comes from file
    cfg2 = resolve_node_config(config_path=conf_file, env_override=env_overrides)
    assert cfg2.host == "env-host"
    assert cfg2.port == 6000
    assert cfg2.peer_id == "env-id"
    assert cfg2.timeout == 10.0

    # Case 3: File + Env + CLI -> CLI overrides both Env and File
    cli_args = argparse.Namespace(
        host="cli-host",
        port=7000,
        peer_id=None,  # peer_id not passed on CLI -> should take env
        config_path=str(conf_file),
    )
    cfg3 = resolve_node_config(args=cli_args, env_override=env_overrides)
    assert cfg3.host == "cli-host"  # from CLI
    assert cfg3.port == 7000  # from CLI
    assert cfg3.peer_id == "env-id"  # from ENV
    assert cfg3.timeout == 10.0  # from File
    assert cfg3.seeds == ["1.1.1.1:1000"]  # from File


# =============================================================================
# 4. Configuration Validation Tests
# =============================================================================


def test_validate_node_config_invalid_port() -> None:
    """Port out of range [0, 65535] raises ConfigurationError."""
    cfg = NodeConfig(port=70000)
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg)
    assert "Invalid port" in str(exc_info.value)

    cfg_neg = NodeConfig(port=-1)
    with pytest.raises(ConfigurationError):
        validate_node_config(cfg_neg)


def test_validate_node_config_empty_host() -> None:
    """Empty host raises ConfigurationError."""
    cfg = NodeConfig(host="")
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg)
    assert "host' must be a non-empty string" in str(exc_info.value)


def test_validate_node_config_non_positive_timeout() -> None:
    """Timeout <= 0 raises ConfigurationError."""
    cfg = NodeConfig(timeout=0)
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg)
    assert "timeout' must be a positive number" in str(exc_info.value)

    cfg_neg = NodeConfig(timeout=-2.5)
    with pytest.raises(ConfigurationError):
        validate_node_config(cfg_neg)


def test_validate_node_config_invalid_seed_format() -> None:
    """Invalid seed address format raises ConfigurationError."""
    cfg_no_port = NodeConfig(seeds=["localhost"])
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg_no_port)
    assert "Must be in format 'host:port'" in str(exc_info.value)

    cfg_bad_port = NodeConfig(seeds=["127.0.0.1:999999"])
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg_bad_port)
    assert "Invalid seed port" in str(exc_info.value)


def test_validate_node_config_missing_cert_file() -> None:
    """Enabling TLS with a non-existent cert file raises ConfigurationError."""
    cfg = NodeConfig(
        security=SecurityConfig(
            tls=TLSConfig(
                enabled=True,
                certfile="nonexistent_cert_path.pem",
            )
        )
    )
    with pytest.raises(ConfigurationError) as exc_info:
        validate_node_config(cfg)
    assert "TLS certificate file does not exist" in str(exc_info.value)


# =============================================================================
# 5. CLI Entrypoint and Startup Error Reporting Tests
# =============================================================================


def test_installed_entry_point_metadata() -> None:
    """Verify that the 'meshweaver' console script entry point is properly registered and loadable."""
    import importlib.metadata as md

    eps = md.entry_points(group="console_scripts")
    mw_eps = [e for e in eps if e.name == "meshweaver"]
    assert len(mw_eps) >= 1, "Expected 'meshweaver' entry point in console_scripts"
    assert mw_eps[0].value == "meshweaver.__main__:main"
    entry_fn = mw_eps[0].load()
    assert callable(entry_fn)


def test_cli_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """`meshweaver --version` prints the version and exits 0."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "0.1.0" in captured.out or "0.1.0" in captured.err


def test_cli_default_banner(capsys: pytest.CaptureFixture[str]) -> None:
    """`meshweaver` without arguments prints the banner and exits 0."""
    exit_code = main([])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "MeshWeaver: Zero-Dependency P2P Async Task Broker" in captured.out


def test_cli_startup_error_missing_config(capsys: pytest.CaptureFixture[str]) -> None:
    """Missing config file prints clean error message without raw traceback and exits with code 2."""
    exit_code = main(["status", "--config", "nonexistent_config_file.json"])
    assert exit_code == 2
    captured = capsys.readouterr()
    assert "[MeshWeaver Configuration Error]" in captured.err
    assert "Configuration file not found" in captured.err
    assert "Traceback" not in captured.err


def test_cli_startup_error_invalid_port(capsys: pytest.CaptureFixture[str]) -> None:
    """Invalid port prints clean configuration error and exits with code 2."""
    exit_code = main(["status", "--port", "99999"])
    assert exit_code == 2
    captured = capsys.readouterr()
    assert "[MeshWeaver Configuration Error]" in captured.err
    assert "Invalid port: 99999" in captured.err
    assert "Traceback" not in captured.err


def test_cli_startup_error_invalid_seed(capsys: pytest.CaptureFixture[str]) -> None:
    """Malformed seed endpoint prints clean configuration error and exits with code 2."""
    exit_code = main(["status", "--seeds", "invalid_seed_format"])
    assert exit_code == 2
    captured = capsys.readouterr()
    assert "[MeshWeaver Configuration Error]" in captured.err
    assert "Invalid seed endpoint" in captured.err
    assert "Traceback" not in captured.err


def test_cli_startup_error_missing_tls_cert(capsys: pytest.CaptureFixture[str]) -> None:
    """Missing TLS certificate prints clean configuration error and exits with code 2."""
    exit_code = main(["status", "--tls", "--cert", "missing_cert.pem"])
    assert exit_code == 2
    captured = capsys.readouterr()
    assert "[MeshWeaver Configuration Error]" in captured.err
    assert "TLS certificate file does not exist" in captured.err
    assert "Traceback" not in captured.err


def test_cli_submit_missing_seeds(capsys: pytest.CaptureFixture[str]) -> None:
    """`meshweaver submit` without seeds raises a clean configuration error."""
    # Ensure env MESHWEAVER_SEEDS is not set
    with patch.dict(os.environ, {}, clear=True):
        exit_code = main(["submit", "add", "1", "2"])
    assert exit_code == 2
    captured = capsys.readouterr()
    assert "[MeshWeaver Configuration Error]" in captured.err
    assert "Seeds are required to submit tasks" in captured.err
    assert "Traceback" not in captured.err
