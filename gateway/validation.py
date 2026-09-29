"""Strict JSON validation for ESP32 telemetry messages."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any


MIN_TIMESTAMP_MS = 1_577_836_800_000  # 2020-01-01T00:00:00Z
MAX_FUTURE_SKEW_MS = 5 * 60 * 1000
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


class TelemetryValidationError(ValueError):
    """A validation error with a stable machine-readable code."""

    def __init__(self, code: str, message: str, *, device_id: str | None = None):
        super().__init__(message)
        self.code = code
        self.device_id = device_id


@dataclass(frozen=True, slots=True)
class Telemetry:
    schema_version: int
    device_id: str
    boot_id: str
    sequence: int
    sent_at_ms: int
    temperature_c: float
    humidity_pct: float
    distance_cm: float
    rssi_dbm: int | None = None
    uptime_s: int | None = None
    led_on: bool | None = None

    @property
    def message_key(self) -> tuple[str, str, int]:
        return self.device_id, self.boot_id, self.sequence


def _reject_nonstandard_number(value: str) -> None:
    raise ValueError(f"non-standard JSON number: {value}")


def _required(data: dict[str, Any], name: str, device_id: str | None) -> Any:
    if name not in data:
        raise TelemetryValidationError(
            "missing_field", f"Missing required field: {name}", device_id=device_id
        )
    return data[name]


def _identifier(data: dict[str, Any], name: str, device_id: str | None) -> str:
    value = _required(data, name, device_id)
    if not isinstance(value, str):
        raise TelemetryValidationError(
            "invalid_type", f"{name} must be a string", device_id=device_id
        )
    if not _IDENTIFIER.fullmatch(value):
        raise TelemetryValidationError(
            "invalid_value",
            f"{name} must be 1-64 safe identifier characters",
            device_id=device_id,
        )
    return value


def _integer(
    data: dict[str, Any],
    name: str,
    device_id: str | None,
    *,
    minimum: int,
    maximum: int,
    required: bool = True,
) -> int | None:
    if not required and name not in data:
        return None
    value = _required(data, name, device_id)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TelemetryValidationError(
            "invalid_type", f"{name} must be an integer", device_id=device_id
        )
    if not minimum <= value <= maximum:
        raise TelemetryValidationError(
            "out_of_range",
            f"{name} must be between {minimum} and {maximum}",
            device_id=device_id,
        )
    return value


def _number(
    data: dict[str, Any],
    name: str,
    device_id: str | None,
    *,
    minimum: float,
    maximum: float,
) -> float:
    value = _required(data, name, device_id)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TelemetryValidationError(
            "invalid_type", f"{name} must be a number", device_id=device_id
        )
    result = float(value)
    if not math.isfinite(result):
        raise TelemetryValidationError(
            "invalid_value", f"{name} must be finite", device_id=device_id
        )
    if not minimum <= result <= maximum:
        raise TelemetryValidationError(
            "out_of_range",
            f"{name} must be between {minimum:g} and {maximum:g}",
            device_id=device_id,
        )
    return result


def parse_telemetry(
    payload: bytes | str,
    *,
    now_ms: int | None = None,
    expected_device_id: str | None = None,
    max_future_skew_ms: int = MAX_FUTURE_SKEW_MS,
) -> Telemetry:
    """Decode and validate one telemetry payload.

    Old, delayed messages remain valid, but timestamps before 2020 or more than
    five minutes in the future are rejected. Keeping delayed messages allows QoS
    delivery after a short network outage while still catching an unsynchronised
    device clock.
    """

    if isinstance(payload, bytes):
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise TelemetryValidationError(
                "invalid_encoding", "Payload must be valid UTF-8"
            ) from exc
    elif isinstance(payload, str):
        text = payload
    else:
        raise TelemetryValidationError(
            "invalid_type", "Payload must be bytes or a string"
        )

    try:
        decoded = json.loads(text, parse_constant=_reject_nonstandard_number)
    except (json.JSONDecodeError, ValueError) as exc:
        raise TelemetryValidationError("invalid_json", f"Invalid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise TelemetryValidationError("invalid_json", "Payload must be a JSON object")

    hinted_device = decoded.get("device_id")
    error_device = hinted_device if isinstance(hinted_device, str) else None
    device_id = _identifier(decoded, "device_id", error_device)
    if expected_device_id is not None and device_id != expected_device_id:
        raise TelemetryValidationError(
            "device_mismatch",
            f"Payload device_id {device_id!r} does not match topic device "
            f"{expected_device_id!r}",
            device_id=device_id,
        )

    boot_id = _identifier(decoded, "boot_id", device_id)
    schema_version = _integer(
        decoded,
        "schema_version",
        device_id,
        minimum=1,
        maximum=32_767,
    )
    sequence = _integer(
        decoded,
        "sequence",
        device_id,
        minimum=0,
        maximum=9_223_372_036_854_775_807,
    )
    sent_at_ms = _integer(
        decoded,
        "sent_at_ms",
        device_id,
        minimum=MIN_TIMESTAMP_MS,
        maximum=32_503_680_000_000,
    )
    assert schema_version is not None
    assert sequence is not None
    assert sent_at_ms is not None

    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    if sent_at_ms > current_ms + max_future_skew_ms:
        raise TelemetryValidationError(
            "future_timestamp",
            f"sent_at_ms is more than {max_future_skew_ms} ms in the future",
            device_id=device_id,
        )

    rssi_dbm = _integer(
        decoded,
        "rssi_dbm",
        device_id,
        minimum=-127,
        maximum=0,
        required=False,
    )
    uptime_s = _integer(
        decoded,
        "uptime_s",
        device_id,
        minimum=0,
        maximum=4_294_967_295,
        required=False,
    )
    led_on: bool | None = None
    if "led_on" in decoded:
        value = decoded["led_on"]
        if not isinstance(value, bool):
            raise TelemetryValidationError(
                "invalid_type", "led_on must be a boolean", device_id=device_id
            )
        led_on = value

    return Telemetry(
        schema_version=schema_version,
        device_id=device_id,
        boot_id=boot_id,
        sequence=sequence,
        sent_at_ms=sent_at_ms,
        temperature_c=_number(
            decoded, "temperature_c", device_id, minimum=-40, maximum=80
        ),
        humidity_pct=_number(
            decoded, "humidity_pct", device_id, minimum=0, maximum=100
        ),
        distance_cm=_number(
            decoded, "distance_cm", device_id, minimum=2, maximum=400
        ),
        rssi_dbm=rssi_dbm,
        uptime_s=uptime_s,
        led_on=led_on,
    )
