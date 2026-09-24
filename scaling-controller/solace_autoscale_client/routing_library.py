"""Trusted application-registered business routing evaluators for managed messaging."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias

MAX_ROUTING_KEY_BYTES = 1024
MAX_HEADERS = 64
MAX_HEADER_NAME_BYTES = 128
MAX_HEADER_VALUE_BYTES = 4096
MAX_HEADERS_BYTES = 32768


class MessageView(Protocol):
    """Whole borrowed message passed to an evaluator before JSON serialization.

    The view and its payload/headers are application-owned objects. Evaluators must treat them as
    read-only and must not retain them after returning. Python cannot enforce immutability here.
    """

    topic: str
    headers: Mapping[str, str]
    payload: Any
    event_id: str


@dataclass(frozen=True)
class BusinessKey:
    """UTF-8 business key hashed with the existing sha256-json-v1 partition contract."""

    value: str


@dataclass(frozen=True)
class SHA256Digest:
    """Exact 32-byte digest mapped directly, big-endian, modulo partition count."""

    value: bytes

    @classmethod
    def from_hex(cls, value: str) -> SHA256Digest:
        if not isinstance(value, str) or len(value) != 64:
            raise RoutingEvaluationError("SHA-256 hex result must contain exactly 64 lowercase hex digits")
        try:
            raw = bytes.fromhex(value)
        except ValueError as exc:
            raise RoutingEvaluationError(
                "SHA-256 hex result must contain exactly 64 lowercase hex digits"
            ) from exc
        if value != raw.hex():
            raise RoutingEvaluationError("SHA-256 hex result must use canonical lowercase hex")
        return cls(raw)


RoutingResult: TypeAlias = BusinessKey | SHA256Digest


class RoutingEvaluator(Protocol):
    def __call__(self, message: MessageView) -> RoutingResult: ...


@dataclass(frozen=True)
class EvaluatorIdentity:
    name: str
    version: str


class RoutingEvaluationError(ValueError):
    """A deterministic local evaluator rejected a publication or delivered message."""


@dataclass(frozen=True)
class ResolvedRouting:
    kind: Literal["key", "sha256"]
    value: str
    partition: int


class RoutingEvaluatorRegistry:
    """Explicit registry of trusted evaluator code installed with the application."""

    def __init__(self) -> None:
        self._evaluators: dict[EvaluatorIdentity, RoutingEvaluator] = {}

    def register(
        self, name: str, version: str, evaluator: RoutingEvaluator
    ) -> RoutingEvaluatorRegistry:
        identity = validate_identity(name, version)
        if not callable(evaluator):
            raise TypeError("routing evaluator must be callable")
        if identity in self._evaluators:
            raise ValueError(f"routing evaluator {name!r} version {version!r} is already registered")
        self._evaluators[identity] = evaluator
        return self

    def require(self, name: object, version: object) -> RoutingEvaluator:
        identity = validate_identity(name, version)
        try:
            return self._evaluators[identity]
        except KeyError as exc:
            raise ValueError(
                f"required routing evaluator {name!r} version {version!r} is not registered"
            ) from exc

    def identities(self) -> Iterator[EvaluatorIdentity]:
        return iter(self._evaluators)

    def evaluate(
        self,
        name: str,
        version: str,
        result_format: Literal["key", "sha256"],
        message: MessageView,
        *,
        shard: str,
        partitions: int,
        key_partition: Callable[[str], int],
    ) -> ResolvedRouting:
        evaluator = self.require(name, version)
        try:
            result = evaluator(message)
        except Exception as exc:
            raise RoutingEvaluationError(
                f"routing evaluator {name!r} version {version!r} failed"
            ) from exc
        if result_format == "key":
            if not isinstance(result, BusinessKey):
                raise RoutingEvaluationError("routing evaluator must return BusinessKey for result: key")
            value = validate_key(result.value)
            return ResolvedRouting("key", value, key_partition(value))
        if result_format == "sha256":
            if not isinstance(result, SHA256Digest):
                raise RoutingEvaluationError(
                    "routing evaluator must return SHA256Digest for result: sha256"
                )
            digest = validate_digest(result.value)
            return ResolvedRouting(
                "sha256", digest.hex(), int.from_bytes(digest, "big") % partitions
            )
        raise RoutingEvaluationError(f"unsupported routing evaluator result {result_format!r}")


def validate_identity(name: object, version: object) -> EvaluatorIdentity:
    if not isinstance(name, str) or not isinstance(version, str):
        raise ValueError("routing evaluator name/version must be strings")
    if (
        not 1 <= len(name.encode("utf-8")) <= 128
        or not 1 <= len(version.encode("utf-8")) <= 64
        or "\x00" in name
        or "\x00" in version
    ):
        raise ValueError("routing evaluator name/version must be bounded nonempty UTF-8 strings")
    return EvaluatorIdentity(name, version)


def validate_key(key: object) -> str:
    if not isinstance(key, str):
        raise RoutingEvaluationError("BusinessKey value must be a string")
    size = len(key.encode("utf-8"))
    if not 1 <= size <= MAX_ROUTING_KEY_BYTES or "\x00" in key:
        raise RoutingEvaluationError(
            f"routing key must be 1-{MAX_ROUTING_KEY_BYTES} UTF-8 bytes without NUL"
        )
    return key


def validate_digest(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise RoutingEvaluationError("SHA256Digest value must contain exactly 32 bytes")
    return value


def validate_headers(headers: Mapping[str, str]) -> None:
    if len(headers) > MAX_HEADERS:
        raise ValueError(f"message headers exceed {MAX_HEADERS} entries")
    total = 0
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("message headers must contain string names and values")
        name_size, value_size = len(name.encode("utf-8")), len(value.encode("utf-8"))
        if not 1 <= name_size <= MAX_HEADER_NAME_BYTES or "\x00" in name:
            raise ValueError("message header name is empty, too long, or contains NUL")
        if value_size > MAX_HEADER_VALUE_BYTES or "\x00" in value:
            raise ValueError("message header value is too long or contains NUL")
        total += name_size + value_size
    if total > MAX_HEADERS_BYTES:
        raise ValueError(f"message headers exceed {MAX_HEADERS_BYTES} UTF-8 bytes")


EvaluatorFunction = Callable[[MessageView], RoutingResult]
