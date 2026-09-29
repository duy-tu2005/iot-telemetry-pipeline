"""Service-independent telemetry processing and sequence tracking."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Protocol

from .validation import Telemetry, TelemetryValidationError, parse_telemetry


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineEvent:
    event_type: str
    severity: str
    received_at_ms: int
    message: str
    device_id: str | None = None
    boot_id: str | None = None
    sequence: int | None = None
    details: dict[str, int | float | str | bool] | None = None


class TelemetrySink(Protocol):
    def write_telemetry(
        self, telemetry: Telemetry, *, received_at_ms: int, latency_ms: int
    ) -> None: ...

    def write_event(self, event: PipelineEvent) -> None: ...


@dataclass(frozen=True, slots=True)
class SequenceStatus:
    duplicate: bool = False
    gap_size: int = 0
    expected_sequence: int | None = None
    out_of_order: bool = False
    latest_sequence: int | None = None


class SequenceTracker:
    """Bounded in-memory duplicate and sequence detector.

    InfluxDB writes are idempotent for the same series timestamp as an extra
    safety net. The cache handles duplicates during the running gateway session.
    """

    def __init__(self, max_entries: int = 20_000):
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._seen: OrderedDict[tuple[str, str, int], None] = OrderedDict()
        self._latest: OrderedDict[tuple[str, str], int] = OrderedDict()
        self._lock = Lock()

    def inspect(self, telemetry: Telemetry) -> SequenceStatus:
        with self._lock:
            if telemetry.message_key in self._seen:
                return SequenceStatus(duplicate=True)

            stream_key = (telemetry.device_id, telemetry.boot_id)
            latest = self._latest.get(stream_key)
            if latest is None:
                return SequenceStatus()
            if telemetry.sequence > latest:
                gap = telemetry.sequence - latest - 1
                return SequenceStatus(
                    gap_size=gap,
                    expected_sequence=latest + 1 if gap else None,
                    latest_sequence=latest,
                )
            return SequenceStatus(
                out_of_order=True,
                expected_sequence=latest + 1,
                latest_sequence=latest,
            )

    def record(self, telemetry: Telemetry) -> None:
        with self._lock:
            key = telemetry.message_key
            self._seen[key] = None
            self._seen.move_to_end(key)
            while len(self._seen) > self._max_entries:
                self._seen.popitem(last=False)

            stream_key = (telemetry.device_id, telemetry.boot_id)
            current = self._latest.get(stream_key)
            if current is None or telemetry.sequence > current:
                self._latest[stream_key] = telemetry.sequence
            self._latest.move_to_end(stream_key)
            while len(self._latest) > self._max_entries:
                self._latest.popitem(last=False)


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    status: str
    device_id: str | None = None
    sequence: int | None = None
    latency_ms: int | None = None
    gap_size: int = 0
    error_code: str | None = None


def topic_device_id(topic: str) -> str | None:
    """Return the device segment for ``iot/lab2/<device>/telemetry``."""

    segments = topic.split("/")
    if len(segments) == 4 and segments[:2] == ["iot", "lab2"]:
        if segments[2] and segments[3] == "telemetry":
            return segments[2]
    return None


class TelemetryProcessor:
    def __init__(
        self,
        sink: TelemetrySink,
        *,
        tracker: SequenceTracker | None = None,
        logger: logging.Logger | None = None,
    ):
        self._sink = sink
        self._tracker = tracker or SequenceTracker()
        self._logger = logger or LOGGER

    def _write_event_safely(self, event: PipelineEvent) -> None:
        try:
            self._sink.write_event(event)
        except Exception:
            self._logger.exception(
                "Could not persist pipeline event type=%s", event.event_type
            )

    def process(
        self,
        payload: bytes | str,
        *,
        topic: str | None = None,
        received_at_ms: int | None = None,
    ) -> ProcessingResult:
        received = int(time.time() * 1000) if received_at_ms is None else received_at_ms
        expected_device = topic_device_id(topic) if topic is not None else None
        if topic is not None and expected_device is None:
            error = TelemetryValidationError(
                "invalid_topic", f"Unexpected telemetry topic: {topic}"
            )
            return self._reject(error, payload, received)

        try:
            telemetry = parse_telemetry(
                payload,
                now_ms=received,
                expected_device_id=expected_device,
            )
        except TelemetryValidationError as exc:
            return self._reject(exc, payload, received)

        sequence_status = self._tracker.inspect(telemetry)
        if sequence_status.duplicate:
            self._write_event_safely(
                PipelineEvent(
                    event_type="duplicate",
                    severity="warning",
                    received_at_ms=received,
                    message="Duplicate telemetry message was ignored",
                    device_id=telemetry.device_id,
                    boot_id=telemetry.boot_id,
                    sequence=telemetry.sequence,
                )
            )
            self._logger.warning(
                "Duplicate ignored device=%s boot=%s sequence=%d",
                telemetry.device_id,
                telemetry.boot_id,
                telemetry.sequence,
            )
            return ProcessingResult(
                status="duplicate",
                device_id=telemetry.device_id,
                sequence=telemetry.sequence,
            )

        latency_ms = received - telemetry.sent_at_ms
        try:
            self._sink.write_telemetry(
                telemetry,
                received_at_ms=received,
                latency_ms=latency_ms,
            )
        except Exception as exc:
            self._logger.exception(
                "InfluxDB telemetry write failed device=%s sequence=%d",
                telemetry.device_id,
                telemetry.sequence,
            )
            self._write_event_safely(
                PipelineEvent(
                    event_type="write_error",
                    severity="error",
                    received_at_ms=received,
                    message="Failed to persist telemetry",
                    device_id=telemetry.device_id,
                    boot_id=telemetry.boot_id,
                    sequence=telemetry.sequence,
                    details={"error": str(exc)[:300]},
                )
            )
            return ProcessingResult(
                status="write_failed",
                device_id=telemetry.device_id,
                sequence=telemetry.sequence,
                latency_ms=latency_ms,
                error_code="influx_write_failed",
            )

        # Only remember a message after its raw point was accepted by the sink.
        # A transient write failure can therefore be retried without being dropped
        # as a duplicate.
        self._tracker.record(telemetry)

        if sequence_status.gap_size:
            self._write_event_safely(
                PipelineEvent(
                    event_type="sequence_gap",
                    severity="warning",
                    received_at_ms=received,
                    message=f"Detected {sequence_status.gap_size} missing message(s)",
                    device_id=telemetry.device_id,
                    boot_id=telemetry.boot_id,
                    sequence=telemetry.sequence,
                    details={
                        "expected_sequence": sequence_status.expected_sequence or 0,
                        "received_sequence": telemetry.sequence,
                        "missing_count": sequence_status.gap_size,
                    },
                )
            )
            self._logger.warning(
                "Sequence gap device=%s boot=%s expected=%d received=%d missing=%d",
                telemetry.device_id,
                telemetry.boot_id,
                sequence_status.expected_sequence,
                telemetry.sequence,
                sequence_status.gap_size,
            )
        elif sequence_status.out_of_order:
            self._write_event_safely(
                PipelineEvent(
                    event_type="out_of_order",
                    severity="warning",
                    received_at_ms=received,
                    message="Telemetry arrived out of sequence",
                    device_id=telemetry.device_id,
                    boot_id=telemetry.boot_id,
                    sequence=telemetry.sequence,
                    details={
                        "latest_sequence": sequence_status.latest_sequence or 0,
                        "received_sequence": telemetry.sequence,
                    },
                )
            )

        self._logger.info(
            "Accepted device=%s sequence=%d latency_ms=%d",
            telemetry.device_id,
            telemetry.sequence,
            latency_ms,
        )
        return ProcessingResult(
            status="accepted",
            device_id=telemetry.device_id,
            sequence=telemetry.sequence,
            latency_ms=latency_ms,
            gap_size=sequence_status.gap_size,
        )

    def _reject(
        self,
        error: TelemetryValidationError,
        payload: bytes | str,
        received_at_ms: int,
    ) -> ProcessingResult:
        if isinstance(payload, bytes):
            raw_payload = payload.decode("utf-8", errors="replace")
        else:
            raw_payload = str(payload)
        self._write_event_safely(
            PipelineEvent(
                event_type="invalid_payload",
                severity="error",
                received_at_ms=received_at_ms,
                message=str(error),
                device_id=error.device_id,
                details={
                    "error_code": error.code,
                    "payload_preview": raw_payload[:512],
                },
            )
        )
        self._logger.warning(
            "Rejected telemetry code=%s device=%s reason=%s",
            error.code,
            error.device_id or "unknown",
            error,
        )
        return ProcessingResult(
            status="rejected",
            device_id=error.device_id,
            error_code=error.code,
        )
