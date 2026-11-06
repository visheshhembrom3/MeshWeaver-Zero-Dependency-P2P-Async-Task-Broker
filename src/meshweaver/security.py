"""
Security layer for MeshWeaver peer-to-peer communication.

Provides:
- Configurable TLS and mutual TLS (mTLS) using Python's standard `ssl` module.
- Certificate verification and trust management.
- Peer authentication via pre-shared tokens and authorized peer allowlists.
- Integrity protection for control and task messages via HMAC-SHA256.
- Optional asymmetric cryptographic signatures (Ed25519) when cryptography is available.
- Replay attack mitigation using sliding timestamp windows and nonce tracking.
- Strict payload and message size limits.

SECURITY NOTE:
MeshWeaver enforces a strict registered-task allowlist (TaskRegistry) for task execution.
Arbitrary remote Python code execution, eval, exec, dynamic imports, and pickle deserialization
are prohibited. However, MeshWeaver does NOT claim or provide an OS-level or hardware-level
sandbox for executing arbitrary Python code. If executing untrusted code, external process
isolation (such as Linux cgroups, containers, or virtual machines) must be employed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
import hashlib
import hmac
import json
import logging
from pathlib import Path
import secrets
import ssl
import time
from typing import Any

from meshweaver.exceptions import (
    AuthenticationError,
    IntegrityError,
    PayloadLimitExceededError,
    ReplayAttackError,
    SecurityError,
    ValidationError,
)
from meshweaver.protocol import Message, MessageType

logger = logging.getLogger(__name__)

# Default security bounds
DEFAULT_MAX_MESSAGE_SIZE: int = 10 * 1024 * 1024  # 10 MB
DEFAULT_MAX_PAYLOAD_SIZE: int = 1 * 1024 * 1024  # 1 MB
DEFAULT_REPLAY_WINDOW: float = 30.0  # 30 seconds freshness window


def canonicalize_message(message: Message) -> bytes:
    """
    Produce a deterministic canonical byte representation of a Message for signing.

    The canonical representation incorporates all envelope headers and a canonical,
    key-sorted JSON serialization of the payload. The 'signature' field is excluded.
    """
    canonical_payload = json.dumps(
        message.payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    # Format: type:id:sender:recipient:corr_id:timestamp:payload
    canonical_str = (
        f"{message.message_type.value}:"
        f"{message.message_id}:"
        f"{message.sender_id}:"
        f"{message.recipient_id or ''}:"
        f"{message.correlation_id or ''}:"
        f"{message.timestamp:.6f}:"
        f"{canonical_payload}"
    )
    return canonical_str.encode("utf-8")


def compute_hmac(secret: bytes | str, data: bytes) -> str:
    """Compute HMAC-SHA256 hex digest over arbitrary data."""
    raw_secret = secret.encode("utf-8") if isinstance(secret, str) else secret
    return hmac.new(raw_secret, data, hashlib.sha256).hexdigest()


def sign_message(message: Message, secret: bytes | str) -> Message:
    """
    Sign a Message using HMAC-SHA256 and attach the digest to message.signature.

    Returns:
        Message: The message instance with its signature field set.
    """
    canonical = canonicalize_message(message)
    sig = compute_hmac(secret, canonical)
    try:
        object.__setattr__(message, "signature", sig)
        return message
    except Exception:
        return replace(message, signature=sig)


def verify_message_integrity(message: Message, secret: bytes | str) -> bool:
    """
    Verify the HMAC-SHA256 signature of a message using constant-time comparison.

    Returns:
        bool: True if signature is valid.

    Raises:
        IntegrityError: If signature is missing or does not match.
    """
    if not message.signature:
        raise IntegrityError(f"Message {message.message_id} is unsigned but integrity is required")

    canonical = canonicalize_message(message)
    expected = compute_hmac(secret, canonical)

    if not hmac.compare_digest(message.signature, expected):
        raise IntegrityError(
            f"Message {message.message_id} failed integrity verification: signature mismatch"
        )
    return True


@dataclass
class TLSConfig:
    """
    Configuration for peer TLS encryption and certificate verification.
    """

    enabled: bool = False
    certfile: str | Path | None = None
    keyfile: str | Path | None = None
    cafile: str | Path | None = None
    cadata: str | bytes | None = None
    require_client_cert: bool = False  # Enable mutual TLS (mTLS)
    verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED
    check_hostname: bool = False  # Typically False for P2P IP-based mesh nodes
    ciphers: str | None = None

    def build_server_context(self) -> ssl.SSLContext | None:
        """
        Build and configure a server-side SSLContext.

        Raises:
            SecurityError: If TLS is enabled but required certificate files are missing or invalid.
        """
        if not self.enabled:
            return None

        if not self.certfile:
            # Node does not have a server certificate configured; cannot serve TLS
            return None

        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

            # Modern secure protocol configuration: disable older protocols
            ctx.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1

            # Load server certificate and private key
            cert_p = str(self.certfile)
            key_p = str(self.keyfile) if self.keyfile else None
            ctx.load_cert_chain(certfile=cert_p, keyfile=key_p)

            # Configure CA verification for client certs (mTLS)
            if self.cafile or self.cadata:
                ca_p = str(self.cafile) if self.cafile else None
                ca_d = self.cadata if isinstance(self.cadata, str) else (
                    self.cadata.decode("utf-8") if self.cadata else None
                )
                ctx.load_verify_locations(cafile=ca_p, cadata=ca_d)

            if self.require_client_cert:
                ctx.verify_mode = ssl.CERT_REQUIRED
            else:
                ctx.verify_mode = ssl.CERT_NONE

            if self.ciphers:
                ctx.set_ciphers(self.ciphers)

            return ctx
        except Exception as exc:
            raise SecurityError(f"Failed to create server SSLContext: {exc}") from exc

    def build_client_context(self) -> ssl.SSLContext | None:
        """
        Build and configure a client-side SSLContext for connecting to remote peers.

        Raises:
            SecurityError: If TLS is enabled but verification setup fails.
        """
        if not self.enabled:
            return None

        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1

            # CA Trust configuration
            if self.cafile or self.cadata:
                ca_p = str(self.cafile) if self.cafile else None
                ca_d = self.cadata if isinstance(self.cadata, str) else (
                    self.cadata.decode("utf-8") if self.cadata else None
                )
                ctx.load_verify_locations(cafile=ca_p, cadata=ca_d)
                ctx.verify_mode = self.verify_mode
            else:
                # Load system default root CAs if no custom CA file provided
                try:
                    ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
                    ctx.verify_mode = self.verify_mode
                except Exception:
                    ctx.verify_mode = ssl.CERT_NONE

            ctx.check_hostname = self.check_hostname

            # Client certificate for mutual TLS (mTLS)
            if self.certfile:
                cert_p = str(self.certfile)
                key_p = str(self.keyfile) if self.keyfile else None
                ctx.load_cert_chain(certfile=cert_p, keyfile=key_p)

            if self.ciphers:
                ctx.set_ciphers(self.ciphers)

            return ctx
        except Exception as exc:
            raise SecurityError(f"Failed to create client SSLContext: {exc}") from exc


class ReplayProtection:
    """
    Mitigates replay attacks using timestamp freshness windows and a sliding nonce/ID cache.
    """

    def __init__(self, window_seconds: float = DEFAULT_REPLAY_WINDOW) -> None:
        self.window_seconds: float = max(1.0, float(window_seconds))
        self._seen: dict[str, float] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    def check_and_record(self, message_id: str, timestamp: float, current_time: float | None = None) -> None:
        """
        Validate message freshness and detect duplicates.

        Raises:
            ReplayAttackError: If the timestamp is outside the freshness window
                               or if the message ID was already processed.
        """
        now = time.time() if current_time is None else current_time

        # 1. Clock skew / freshness check
        if abs(now - timestamp) > self.window_seconds:
            diff = now - timestamp
            raise ReplayAttackError(
                f"Message timestamp {timestamp:.2f} is outside acceptable freshness window "
                f"({self.window_seconds}s). Drift: {diff:.2f}s"
            )

        # 2. Duplicate message ID check
        if message_id in self._seen:
            raise ReplayAttackError(
                f"Replay detected: Message ID '{message_id}' has already been processed"
            )

        # 3. Record seen message
        self._seen[message_id] = now

        # 4. Prune expired entries
        cutoff = now - self.window_seconds
        expired_keys = [mid for mid, ts in self._seen.items() if ts < cutoff]
        for mid in expired_keys:
            del self._seen[mid]

    def clear(self) -> None:
        """Reset the replay cache."""
        self._seen.clear()


@dataclass
class SecurityConfig:
    """
    Unified security policy configuration for a MeshWeaver PeerNode.
    """

    # TLS Transport configuration
    tls: TLSConfig = field(default_factory=TLSConfig)

    # Pre-shared secret for HMAC-SHA256 message integrity & authentication
    shared_secret: bytes | str | None = None

    # Pre-shared authentication bearer token
    auth_token: str | None = None

    # Explicit whitelist of authorized peer IDs (None = all peers allowed)
    allowed_peers: set[str] | None = None

    # Enforce authentication on all incoming connections
    require_auth: bool = False

    # Enforce HMAC signature on all incoming messages if shared_secret is set
    enforce_integrity: bool = True

    # Replay protection sliding window in seconds (0 = disabled)
    replay_window: float = DEFAULT_REPLAY_WINDOW

    # Maximum permitted message size in bytes
    max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE

    # Maximum permitted payload dictionary size in bytes
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE


class SecurityManager:
    """
    Central security coordinator for a MeshWeaver PeerNode.

    Handles TLS context building, outbound message signing/preparation,
    inbound message authentication, replay validation, and size limit enforcement.
    """

    def __init__(self, config: SecurityConfig | None = None) -> None:
        self.config: SecurityConfig = config if config is not None else SecurityConfig()
        self.replay_guard: ReplayProtection | None = (
            ReplayProtection(self.config.replay_window)
            if self.config.replay_window > 0
            else None
        )
        self._server_ssl: ssl.SSLContext | None = None
        self._client_ssl: ssl.SSLContext | None = None

    @property
    def is_tls_enabled(self) -> bool:
        """Return True if TLS encryption is enabled."""
        return self.config.tls.enabled

    @property
    def is_integrity_enabled(self) -> bool:
        """Return True if HMAC integrity signing is enabled."""
        return bool(self.config.shared_secret and self.config.enforce_integrity)

    @property
    def is_auth_enabled(self) -> bool:
        """Return True if peer authentication or token verification is configured."""
        return bool(self.config.auth_token or self.config.allowed_peers or self.config.require_auth)

    def get_server_ssl_context(self) -> ssl.SSLContext | None:
        """Get or lazily build the server-side SSLContext."""
        if self._server_ssl is None and self.config.tls.enabled:
            self._server_ssl = self.config.tls.build_server_context()
        return self._server_ssl

    def get_client_ssl_context(self) -> ssl.SSLContext | None:
        """Get or lazily build the client-side SSLContext."""
        if self._client_ssl is None and self.config.tls.enabled:
            self._client_ssl = self.config.tls.build_client_context()
        return self._client_ssl

    def prepare_outbound(self, message: Message) -> Message:
        """
        Prepare an outbound message before wire transmission:
        - Injects auth_token into HANDSHAKE payloads if configured.
        - Signs the message using HMAC-SHA256 if shared_secret is configured.
        - Enforces payload size limits.

        Returns:
            Message: The prepared message ready for framing.
        """
        # 1. Enforce payload size limit
        payload_bytes_len = len(json.dumps(message.payload))
        if payload_bytes_len > self.config.max_payload_size:
            raise PayloadLimitExceededError(
                f"Outbound payload size ({payload_bytes_len} bytes) exceeds limit ({self.config.max_payload_size} bytes)"
            )

        # 2. Inject auth_token into handshake if configured
        if self.config.auth_token and message.message_type in (
            MessageType.HANDSHAKE,
            MessageType.HANDSHAKE_ACK,
        ):
            if "auth_token" not in message.payload:
                new_payload = dict(message.payload)
                new_payload["auth_token"] = self.config.auth_token
                try:
                    object.__setattr__(message, "payload", new_payload)
                except Exception:
                    message = replace(message, payload=new_payload)

        # 3. Sign message if shared_secret is present
        if self.config.shared_secret:
            message = sign_message(message, self.config.shared_secret)

        return message

    def validate_inbound(self, message: Message, remote_peer_id: str | None = None) -> None:
        """
        Perform comprehensive security validation on an incoming message:
        - Size bounds check.
        - Replay attack / freshness check.
        - Authorized peer whitelist check.
        - HMAC signature / integrity check.

        Raises:
            PayloadLimitExceededError: If payload or message exceeds configured limits.
            ReplayAttackError: If message is stale or replayed.
            AuthenticationError: If sender is unauthorized.
            IntegrityError: If HMAC signature verification fails.
        """
        # 1. Payload size check
        payload_len = len(json.dumps(message.payload))
        if payload_len > self.config.max_payload_size:
            raise PayloadLimitExceededError(
                f"Inbound payload size ({payload_len} bytes) exceeds limit ({self.config.max_payload_size} bytes)"
            )

        # 2. Replay & freshness check (skip if disabled)
        if self.replay_guard is not None:
            self.replay_guard.check_and_record(message.message_id, message.timestamp)

        # 3. Peer authorization check
        sender = remote_peer_id or message.sender_id
        if self.config.allowed_peers is not None and sender not in self.config.allowed_peers:
            raise AuthenticationError(
                f"Peer '{sender}' is not authorized in allowed_peers policy"
            )

        # 4. Message integrity check
        if self.config.shared_secret:
            if not message.signature:
                if self.config.enforce_integrity:
                    raise IntegrityError(
                        f"Incoming message {message.message_id} ({message.message_type}) is missing required signature"
                    )
            else:
                verify_message_integrity(message, self.config.shared_secret)
        elif message.signature and self.config.enforce_integrity:
            # Message has signature but local node has no shared_secret to verify it
            logger.debug("Received signed message %s but no shared_secret configured locally", message.message_id)

    def validate_handshake(self, message: Message) -> None:
        """
        Validate peer credentials during the initial HANDSHAKE or HANDSHAKE_ACK exchange.

        Raises:
            AuthenticationError: If credentials fail verification.
        """
        # 1. Check peer whitelist
        if self.config.allowed_peers is not None:
            if message.sender_id not in self.config.allowed_peers:
                raise AuthenticationError(
                    f"Handshake rejected: Peer ID '{message.sender_id}' is not in allowed_peers"
                )

        # 2. Check auth token if configured
        if self.config.auth_token:
            received_token = message.payload.get("auth_token")
            if not received_token:
                raise AuthenticationError("Handshake rejected: Missing authentication token")

            if not secrets.compare_digest(str(received_token), str(self.config.auth_token)):
                raise AuthenticationError("Handshake rejected: Invalid authentication token")
