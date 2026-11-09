"""
Configuration management and environment variable resolution for MeshWeaver.

Supports loading and merging configurations with strict priority:
1. Explicit CLI arguments (highest priority)
2. Environment variables (prefixed with MESHWEAVER_)
3. JSON configuration file
4. Sensible built-in defaults (lowest priority)

Provides clear, human-readable startup errors for invalid inputs, missing files,
or malformed parameters without unhandled tracebacks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
from typing import Any

from meshweaver.exceptions import ConfigurationError
from meshweaver.security import SecurityConfig, TLSConfig


def parse_env_bool(val: str | None, default: bool = False) -> bool:
    """Parse a boolean environment variable value."""
    if val is None:
        return default
    cleaned = val.strip().lower()
    if cleaned in ("1", "true", "yes", "on", "enable", "enabled"):
        return True
    if cleaned in ("0", "false", "no", "off", "disable", "disabled"):
        return False
    return default


def load_config_file(path: str | Path) -> dict[str, Any]:
    """
    Load and parse a JSON configuration file.

    Raises:
        ConfigurationError: If the file does not exist, cannot be read, or is not valid JSON.
    """
    conf_path = Path(path).resolve()
    if not conf_path.exists():
        raise ConfigurationError(f"Configuration file not found: {conf_path}")
    if not conf_path.is_file():
        raise ConfigurationError(f"Configuration path is not a file: {conf_path}")

    try:
        content = conf_path.read_text(encoding="utf-8")
        data = json.loads(content)
    except json.JSONDecodeError as json_err:
        raise ConfigurationError(
            f"Failed to parse JSON configuration in '{conf_path}': line {json_err.lineno}, col {json_err.colno}: {json_err.msg}"
        ) from json_err
    except OSError as os_err:
        raise ConfigurationError(f"Failed to read configuration file '{conf_path}': {os_err}") from os_err

    if not isinstance(data, dict):
        raise ConfigurationError(
            f"Configuration file '{conf_path}' must contain a top-level JSON object"
        )
    return data


def load_env_config(env: dict[str, str] | os._Environ[str] | None = None) -> dict[str, Any]:
    """
    Extract MeshWeaver settings from environment variables.
    Environment variables are prefixed with MESHWEAVER_.
    """
    environ = os.environ if env is None else env
    cfg: dict[str, Any] = {}

    if "MESHWEAVER_HOST" in environ:
        cfg["host"] = environ["MESHWEAVER_HOST"].strip()

    if "MESHWEAVER_PORT" in environ:
        try:
            cfg["port"] = int(environ["MESHWEAVER_PORT"].strip())
        except ValueError:
            raise ConfigurationError(
                f"Environment variable MESHWEAVER_PORT must be an integer, got: {environ['MESHWEAVER_PORT']!r}"
            )

    if "MESHWEAVER_PEER_ID" in environ:
        cfg["peer_id"] = environ["MESHWEAVER_PEER_ID"].strip()

    if "MESHWEAVER_SEEDS" in environ:
        raw_seeds = environ["MESHWEAVER_SEEDS"].strip()
        if raw_seeds:
            cfg["seeds"] = [s.strip() for s in raw_seeds.split(",") if s.strip()]

    if "MESHWEAVER_TIMEOUT" in environ:
        try:
            cfg["timeout"] = float(environ["MESHWEAVER_TIMEOUT"].strip())
        except ValueError:
            raise ConfigurationError(
                f"Environment variable MESHWEAVER_TIMEOUT must be a float, got: {environ['MESHWEAVER_TIMEOUT']!r}"
            )

    if "MESHWEAVER_HEARTBEAT_INTERVAL" in environ:
        try:
            cfg["heartbeat_interval"] = float(environ["MESHWEAVER_HEARTBEAT_INTERVAL"].strip())
        except ValueError:
            raise ConfigurationError(
                f"Environment variable MESHWEAVER_HEARTBEAT_INTERVAL must be a float, got: {environ['MESHWEAVER_HEARTBEAT_INTERVAL']!r}"
            )

    if "MESHWEAVER_MISSED_THRESHOLD" in environ:
        try:
            cfg["missed_threshold"] = int(environ["MESHWEAVER_MISSED_THRESHOLD"].strip())
        except ValueError:
            raise ConfigurationError(
                f"Environment variable MESHWEAVER_MISSED_THRESHOLD must be an integer, got: {environ['MESHWEAVER_MISSED_THRESHOLD']!r}"
            )

    if "MESHWEAVER_GOSSIP_INTERVAL" in environ:
        try:
            cfg["gossip_interval"] = float(environ["MESHWEAVER_GOSSIP_INTERVAL"].strip())
        except ValueError:
            raise ConfigurationError(
                f"Environment variable MESHWEAVER_GOSSIP_INTERVAL must be a float, got: {environ['MESHWEAVER_GOSSIP_INTERVAL']!r}"
            )

    if "MESHWEAVER_VERBOSE" in environ:
        cfg["verbose"] = parse_env_bool(environ["MESHWEAVER_VERBOSE"])

    if "MESHWEAVER_JSON" in environ:
        cfg["as_json"] = parse_env_bool(environ["MESHWEAVER_JSON"])

    if "MESHWEAVER_PLAIN" in environ:
        cfg["force_plain"] = parse_env_bool(environ["MESHWEAVER_PLAIN"])

    # Security settings
    sec: dict[str, Any] = {}
    tls: dict[str, Any] = {}

    if "MESHWEAVER_TLS_ENABLED" in environ:
        tls["enabled"] = parse_env_bool(environ["MESHWEAVER_TLS_ENABLED"])

    cert_file = environ.get("MESHWEAVER_TLS_CERT") or environ.get("MESHWEAVER_CERT_FILE")
    if cert_file:
        tls["certfile"] = cert_file.strip()
        tls["enabled"] = True

    key_file = environ.get("MESHWEAVER_TLS_KEY") or environ.get("MESHWEAVER_KEY_FILE")
    if key_file:
        tls["keyfile"] = key_file.strip()

    ca_file = environ.get("MESHWEAVER_TLS_CA") or environ.get("MESHWEAVER_CA_FILE")
    if ca_file:
        tls["cafile"] = ca_file.strip()
        tls["enabled"] = True

    if "MESHWEAVER_REQUIRE_CLIENT_CERT" in environ:
        tls["require_client_cert"] = parse_env_bool(environ["MESHWEAVER_REQUIRE_CLIENT_CERT"])

    if "MESHWEAVER_SHARED_SECRET" in environ:
        sec["shared_secret"] = environ["MESHWEAVER_SHARED_SECRET"].strip()

    if "MESHWEAVER_AUTH_TOKEN" in environ:
        sec["auth_token"] = environ["MESHWEAVER_AUTH_TOKEN"].strip()

    if "MESHWEAVER_ALLOWED_PEERS" in environ:
        raw_allowed = environ["MESHWEAVER_ALLOWED_PEERS"].strip()
        if raw_allowed:
            sec["allowed_peers"] = {p.strip() for p in raw_allowed.split(",") if p.strip()}

    if tls:
        sec["tls"] = tls
    if sec:
        cfg["security"] = sec

    return cfg


@dataclass
class NodeConfig:
    """Unified application configuration for a MeshWeaver node."""

    host: str = "127.0.0.1"
    port: int = 0
    peer_id: str | None = None
    seeds: list[str] = field(default_factory=list)
    timeout: float = 5.0
    duration: float | None = None
    heartbeat_interval: float = 1.0
    missed_threshold: int = 3
    gossip_interval: float = 5.0
    security: SecurityConfig = field(default_factory=SecurityConfig)
    verbose: bool = False
    as_json: bool = False
    show_dashboard: bool = False
    refresh_rate: float = 1.0
    force_plain: bool = False
    config_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        d = asdict(self)
        d["security"] = self.security.to_dict() if hasattr(self.security, "to_dict") else str(self.security)
        return d


def validate_node_config(cfg: NodeConfig) -> None:
    """
    Validate that configuration values are semantically and operationally valid.

    Raises:
        ConfigurationError: If any parameter is invalid, out of range, or references non-existent files.
    """
    # 1. Validate host
    if not cfg.host or not isinstance(cfg.host, str) or not cfg.host.strip():
        raise ConfigurationError("Configuration 'host' must be a non-empty string")

    # 2. Validate port
    if not isinstance(cfg.port, int) or isinstance(cfg.port, bool) or not (0 <= cfg.port <= 65535):
        raise ConfigurationError(f"Invalid port: {cfg.port}. Must be an integer between 0 and 65535")

    # 3. Validate timeout
    if not isinstance(cfg.timeout, (int, float)) or cfg.timeout <= 0:
        raise ConfigurationError(f"Configuration 'timeout' must be a positive number, got: {cfg.timeout}")

    # 4. Validate heartbeat & gossip intervals
    if not isinstance(cfg.heartbeat_interval, (int, float)) or cfg.heartbeat_interval <= 0:
        raise ConfigurationError(f"Configuration 'heartbeat_interval' must be positive, got: {cfg.heartbeat_interval}")

    if not isinstance(cfg.missed_threshold, int) or isinstance(cfg.missed_threshold, bool) or cfg.missed_threshold <= 0:
        raise ConfigurationError(f"Configuration 'missed_threshold' must be a positive integer, got: {cfg.missed_threshold}")

    if not isinstance(cfg.gossip_interval, (int, float)) or cfg.gossip_interval <= 0:
        raise ConfigurationError(f"Configuration 'gossip_interval' must be positive, got: {cfg.gossip_interval}")

    # 5. Validate seeds format
    for seed in cfg.seeds:
        if not isinstance(seed, str) or ":" not in seed:
            raise ConfigurationError(f"Invalid seed endpoint '{seed}'. Must be in format 'host:port'")
        parts = seed.rsplit(":", 1)
        try:
            sport = int(parts[1])
            if not (1 <= sport <= 65535):
                raise ValueError()
        except ValueError:
            raise ConfigurationError(
                f"Invalid seed port in '{seed}'. Port must be an integer between 1 and 65535"
            )

    # 6. Validate security TLS files if enabled
    tls = cfg.security.tls
    if tls.enabled:
        if tls.certfile:
            cert_p = Path(tls.certfile)
            if not cert_p.exists():
                raise ConfigurationError(f"TLS certificate file does not exist: {cert_p}")
        if tls.keyfile:
            key_p = Path(tls.keyfile)
            if not key_p.exists():
                raise ConfigurationError(f"TLS private key file does not exist: {key_p}")
        if tls.cafile:
            ca_p = Path(tls.cafile)
            if not ca_p.exists():
                raise ConfigurationError(f"TLS CA certificate file does not exist: {ca_p}")


def resolve_node_config(
    args: Any = None,
    config_path: str | Path | None = None,
    env_override: dict[str, str] | None = None,
) -> NodeConfig:
    """
    Resolve complete node configuration using the hierarchy:
    CLI Arguments > Environment Variables > Configuration File > Built-in Defaults.

    Raises:
        ConfigurationError: If validation fails or specified config files do not exist.
    """
    # 1. Start with defaults
    cfg = NodeConfig()

    # 2. Determine configuration file path
    effective_config_path = config_path or getattr(args, "config_path", None)
    if not effective_config_path:
        env_map = os.environ if env_override is None else env_override
        if "MESHWEAVER_CONFIG" in env_map:
            effective_config_path = env_map["MESHWEAVER_CONFIG"].strip()

    file_data: dict[str, Any] = {}
    if effective_config_path:
        file_data = load_config_file(effective_config_path)
        cfg.config_path = str(effective_config_path)

        # Map file_data to config fields
        # Supports both flat and nested JSON schemas
        net_sec = file_data.get("network", {})
        if "host" in file_data:
            cfg.host = str(file_data["host"])
        elif "host" in net_sec:
            cfg.host = str(net_sec["host"])

        if "port" in file_data:
            try:
                cfg.port = int(file_data["port"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration file 'port' must be an integer, got: {file_data['port']!r}")
        elif "port" in net_sec:
            try:
                cfg.port = int(net_sec["port"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration file 'network.port' must be an integer, got: {net_sec['port']!r}")

        if "peer_id" in file_data:
            cfg.peer_id = str(file_data["peer_id"])
        elif "peer_id" in net_sec:
            cfg.peer_id = str(net_sec["peer_id"])

        if "timeout" in file_data:
            try:
                cfg.timeout = float(file_data["timeout"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration file 'timeout' must be a number, got: {file_data['timeout']!r}")
        elif "connection_timeout" in net_sec:
            try:
                cfg.timeout = float(net_sec["connection_timeout"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration file 'network.connection_timeout' must be a number, got: {net_sec['connection_timeout']!r}")

        if "seeds" in file_data and isinstance(file_data["seeds"], list):
            cfg.seeds = [str(s) for s in file_data["seeds"]]
        elif "bootstrap_seeds" in file_data and isinstance(file_data["bootstrap_seeds"], list):
            cfg.seeds = [str(s) for s in file_data["bootstrap_seeds"]]

        hb_sec = file_data.get("heartbeat", {})
        if "interval" in hb_sec:
            try:
                cfg.heartbeat_interval = float(hb_sec["interval"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration 'heartbeat.interval' must be a number, got: {hb_sec['interval']!r}")
        if "missed_threshold" in hb_sec:
            try:
                cfg.missed_threshold = int(hb_sec["missed_threshold"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration 'heartbeat.missed_threshold' must be an integer, got: {hb_sec['missed_threshold']!r}")

        gossip_sec = file_data.get("gossip", {})
        if "interval" in gossip_sec:
            try:
                cfg.gossip_interval = float(gossip_sec["interval"])
            except (ValueError, TypeError):
                raise ConfigurationError(f"Configuration 'gossip.interval' must be a number, got: {gossip_sec['interval']!r}")

        log_sec = file_data.get("logging", {})
        if "verbose" in log_sec:
            cfg.verbose = bool(log_sec["verbose"])
        if "as_json" in log_sec:
            cfg.as_json = bool(log_sec["as_json"])

        # Security from file
        sec_data = file_data.get("security", {})
        if sec_data:
            tls_data = sec_data.get("tls", {})
            tls_cfg = TLSConfig(
                enabled=bool(tls_data.get("enabled", False)),
                certfile=tls_data.get("certfile"),
                keyfile=tls_data.get("keyfile"),
                cafile=tls_data.get("cafile"),
                require_client_cert=bool(tls_data.get("require_client_cert", False)),
                check_hostname=bool(tls_data.get("check_hostname", True)),
            )
            raw_allowed = sec_data.get("allowed_peers")
            allowed = set(raw_allowed) if raw_allowed else None
            cfg.security = SecurityConfig(
                tls=tls_cfg,
                shared_secret=sec_data.get("shared_secret"),
                auth_token=sec_data.get("auth_token"),
                allowed_peers=allowed,
            )

    # 3. Environment variables layer
    env_data = load_env_config(env_override)
    if "host" in env_data:
        cfg.host = env_data["host"]
    if "port" in env_data:
        cfg.port = env_data["port"]
    if "peer_id" in env_data:
        cfg.peer_id = env_data["peer_id"]
    if "seeds" in env_data:
        cfg.seeds = env_data["seeds"]
    if "timeout" in env_data:
        cfg.timeout = env_data["timeout"]
    if "heartbeat_interval" in env_data:
        cfg.heartbeat_interval = env_data["heartbeat_interval"]
    if "missed_threshold" in env_data:
        cfg.missed_threshold = env_data["missed_threshold"]
    if "gossip_interval" in env_data:
        cfg.gossip_interval = env_data["gossip_interval"]
    if "verbose" in env_data:
        cfg.verbose = env_data["verbose"]
    if "as_json" in env_data:
        cfg.as_json = env_data["as_json"]
    if "force_plain" in env_data:
        cfg.force_plain = env_data["force_plain"]

    if "security" in env_data:
        env_sec = env_data["security"]
        if "tls" in env_sec:
            env_tls = env_sec["tls"]
            if "enabled" in env_tls:
                cfg.security.tls.enabled = env_tls["enabled"]
            if "certfile" in env_tls:
                cfg.security.tls.certfile = env_tls["certfile"]
            if "keyfile" in env_tls:
                cfg.security.tls.keyfile = env_tls["keyfile"]
            if "cafile" in env_tls:
                cfg.security.tls.cafile = env_tls["cafile"]
            if "require_client_cert" in env_tls:
                cfg.security.tls.require_client_cert = env_tls["require_client_cert"]
        if "shared_secret" in env_sec:
            cfg.security.shared_secret = env_sec["shared_secret"]
        if "auth_token" in env_sec:
            cfg.security.auth_token = env_sec["auth_token"]
        if "allowed_peers" in env_sec:
            cfg.security.allowed_peers = env_sec["allowed_peers"]

    # 4. Explicit CLI arguments (highest priority)
    if args is not None:
        cli_host = getattr(args, "host", None)
        if cli_host is not None:
            cfg.host = cli_host

        cli_port = getattr(args, "port", None)
        if cli_port is not None:
            cfg.port = cli_port

        cli_peer_id = getattr(args, "peer_id", None)
        if cli_peer_id is not None:
            cfg.peer_id = cli_peer_id

        cli_seeds = getattr(args, "seeds", None)
        if cli_seeds is not None:
            if isinstance(cli_seeds, list):
                cfg.seeds = cli_seeds
            elif isinstance(cli_seeds, str) and cli_seeds.strip():
                cfg.seeds = [s.strip() for s in cli_seeds.split(",") if s.strip()]

        cli_timeout = getattr(args, "timeout", None)
        if cli_timeout is not None:
            cfg.timeout = cli_timeout

        cli_duration = getattr(args, "duration", None)
        if cli_duration is not None:
            cfg.duration = cli_duration

        if getattr(args, "verbose", False):
            cfg.verbose = True
        if getattr(args, "json", False):
            cfg.as_json = True
        if getattr(args, "plain", False):
            cfg.force_plain = True
        if getattr(args, "show_dashboard", False):
            cfg.show_dashboard = True
        if getattr(args, "refresh_rate", None) is not None:
            cfg.refresh_rate = getattr(args, "refresh_rate")

        # Security arguments from CLI
        if getattr(args, "tls", False):
            cfg.security.tls.enabled = True
        if getattr(args, "cert_file", None):
            cfg.security.tls.certfile = getattr(args, "cert_file")
            cfg.security.tls.enabled = True
        if getattr(args, "key_file", None):
            cfg.security.tls.keyfile = getattr(args, "key_file")
        if getattr(args, "ca_file", None):
            cfg.security.tls.cafile = getattr(args, "ca_file")
            cfg.security.tls.enabled = True
        if getattr(args, "require_client_cert", False):
            cfg.security.tls.require_client_cert = True
        if getattr(args, "shared_secret", None):
            cfg.security.shared_secret = getattr(args, "shared_secret")
        if getattr(args, "auth_token", None):
            cfg.security.auth_token = getattr(args, "auth_token")
        if getattr(args, "allowed_peers", None):
            raw = getattr(args, "allowed_peers")
            if isinstance(raw, (set, list)):
                cfg.security.allowed_peers = set(raw)
            elif isinstance(raw, str):
                cfg.security.allowed_peers = {p.strip() for p in raw.split(",") if p.strip()}

    # 5. Semantic and operational validation
    validate_node_config(cfg)
    return cfg
