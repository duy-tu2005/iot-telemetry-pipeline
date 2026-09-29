"""InfluxDB implementation of the processor sink protocol."""

from __future__ import annotations

from typing import Any

from .processor import PipelineEvent
from .validation import Telemetry


class InfluxSink:
    """Write raw telemetry and pipeline diagnostics to one InfluxDB bucket."""

    def __init__(
        self,
        *,
        url: str,
        token: str,
        org: str,
        bucket: str,
        location: str,
    ):
        # Keep third-party imports at the service boundary. Validation and
        # processing tests therefore need neither a server nor this dependency.
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS

        self._org = org
        self._bucket = bucket
        self._location = location
        self._client = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            # The first write can trigger TSM/index creation on a fresh
            # Docker volume, especially on Windows. Keep the gateway alive
            # long enough for that one-time initialization to finish.
            timeout=60_000,
            enable_gzip=True,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def write_telemetry(
        self, telemetry: Telemetry, *, received_at_ms: int, latency_ms: int
    ) -> None:
        from influxdb_client import Point, WritePrecision

        point = (
            Point("sensor_raw")
            .tag("device_id", telemetry.device_id)
            .tag("location", self._location)
            .tag("schema_version", str(telemetry.schema_version))
            .field("boot_id", telemetry.boot_id)
            .field("sequence", telemetry.sequence)
            .field("sent_at_ms", telemetry.sent_at_ms)
            .field("received_at_ms", received_at_ms)
            .field("latency_ms", latency_ms)
            .field("temperature_c", telemetry.temperature_c)
            .field("humidity_pct", telemetry.humidity_pct)
            .field("distance_cm", telemetry.distance_cm)
            .time(telemetry.sent_at_ms, WritePrecision.MS)
        )
        if telemetry.rssi_dbm is not None:
            point.field("rssi_dbm", telemetry.rssi_dbm)
        if telemetry.uptime_s is not None:
            point.field("uptime_s", telemetry.uptime_s)
        if telemetry.led_on is not None:
            point.field("led_on", telemetry.led_on)
        self._write_api.write(bucket=self._bucket, org=self._org, record=point)

    def write_event(self, event: PipelineEvent) -> None:
        from influxdb_client import Point, WritePrecision

        point = (
            Point("pipeline_event")
            .tag("event_type", event.event_type)
            .tag("severity", event.severity)
            .tag("location", self._location)
            .field("message", event.message[:500])
            .time(event.received_at_ms, WritePrecision.MS)
        )
        if event.device_id:
            point.tag("device_id", event.device_id)
        if event.boot_id:
            point.field("boot_id", event.boot_id)
        if event.sequence is not None:
            point.field("sequence", event.sequence)
        for name, value in (event.details or {}).items():
            point.field(name, _influx_field(value))
        self._write_api.write(bucket=self._bucket, org=self._org, record=point)

    def close(self) -> None:
        self._write_api.close()
        self._client.close()


def _influx_field(value: Any) -> int | float | str | bool:
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)
