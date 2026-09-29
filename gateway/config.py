"""Environment based configuration for the laboratory gateway."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class ConfigurationError(ValueError):
    """Raised when a gateway configuration value is missing or invalid."""


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} is required")
    return value


def _text(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _integer(
    values: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = values.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    mqtt_host: str
    mqtt_port: int
    mqtt_topic: str
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_client_id: str
    mqtt_qos: int
    influx_url: str
    influx_token: str
    influx_org: str
    influx_raw_bucket: str
    location: str
    dedupe_cache_size: int
    log_level: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        """Build settings from an environment-like mapping.

        ``environ`` is injectable so configuration can be tested without changing
        the process environment.
        """

        values = os.environ if environ is None else environ
        username = values.get("MQTT_USERNAME", "").strip() or None
        password = values.get("MQTT_PASSWORD", "") or None
        if password is not None and username is None:
            raise ConfigurationError("MQTT_USERNAME is required when MQTT_PASSWORD is set")

        log_level = _text(values, "GATEWAY_LOG_LEVEL", "INFO").upper()
        allowed_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if log_level not in allowed_log_levels:
            raise ConfigurationError(
                "GATEWAY_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
            )

        return cls(
            mqtt_host=_text(values, "MQTT_HOST", "127.0.0.1"),
            mqtt_port=_integer(
                values, "MQTT_PORT", 1884, minimum=1, maximum=65535
            ),
            mqtt_topic=_text(
                values, "MQTT_TOPIC", "iot/lab2/+/telemetry"
            ),
            mqtt_username=username,
            mqtt_password=password,
            mqtt_client_id=_text(
                values, "MQTT_CLIENT_ID", "iot-lab2-python-gateway"
            ),
            mqtt_qos=_integer(values, "MQTT_QOS", 1, minimum=0, maximum=2),
            influx_url=_text(values, "INFLUX_URL", "http://127.0.0.1:8086"),
            influx_token=_required(values, "INFLUX_TOKEN"),
            influx_org=_text(values, "INFLUX_ORG", "iot-lab"),
            influx_raw_bucket=_text(values, "INFLUX_RAW_BUCKET", "iot_raw"),
            location=_text(values, "LOCATION", "lab"),
            dedupe_cache_size=_integer(
                values,
                "DEDUPE_CACHE_SIZE",
                20_000,
                minimum=100,
                maximum=2_000_000,
            ),
            log_level=log_level,
        )
