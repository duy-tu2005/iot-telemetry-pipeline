import json

from gateway.processor import PipelineEvent, SequenceTracker, TelemetryProcessor
from gateway.validation import Telemetry


NOW_MS = 1_800_000_000_000


def payload(sequence=1, *, boot_id="boot-a", **overrides):
    document = {
        "schema_version": 1,
        "device_id": "esp32-01",
        "boot_id": boot_id,
        "sequence": sequence,
        "sent_at_ms": NOW_MS - 125,
        "temperature_c": 24.5,
        "humidity_pct": 50.0,
        "distance_cm": 100.0,
        "rssi_dbm": -55,
        "uptime_s": 10,
        "led_on": False,
    }
    document.update(overrides)
    return json.dumps(document)


class RecordingSink:
    def __init__(self):
        self.telemetry = []
        self.events: list[PipelineEvent] = []

    def write_telemetry(self, telemetry, *, received_at_ms, latency_ms):
        self.telemetry.append((telemetry, received_at_ms, latency_ms))

    def write_event(self, event):
        self.events.append(event)


def process(processor, message, *, topic="iot/lab2/esp32-01/telemetry"):
    return processor.process(message, topic=topic, received_at_ms=NOW_MS)


def test_accepted_message_is_written_with_latency():
    sink = RecordingSink()
    result = process(TelemetryProcessor(sink), payload())

    assert result.status == "accepted"
    assert result.latency_ms == 125
    assert len(sink.telemetry) == 1
    assert sink.telemetry[0][2] == 125
    assert sink.events == []


def test_duplicate_key_is_logged_and_not_written_twice():
    sink = RecordingSink()
    processor = TelemetryProcessor(sink)

    first = process(processor, payload(sequence=7))
    duplicate = process(processor, payload(sequence=7))

    assert first.status == "accepted"
    assert duplicate.status == "duplicate"
    assert len(sink.telemetry) == 1
    assert [event.event_type for event in sink.events] == ["duplicate"]


def test_sequence_gap_reports_number_of_missing_messages():
    sink = RecordingSink()
    processor = TelemetryProcessor(sink)
    process(processor, payload(sequence=10))

    result = process(processor, payload(sequence=14))

    assert result.status == "accepted"
    assert result.gap_size == 3
    gap = sink.events[-1]
    assert gap.event_type == "sequence_gap"
    assert gap.details == {
        "expected_sequence": 11,
        "received_sequence": 14,
        "missing_count": 3,
    }


def test_new_boot_id_starts_an_independent_sequence():
    sink = RecordingSink()
    processor = TelemetryProcessor(sink)
    process(processor, payload(sequence=100, boot_id="boot-a"))

    result = process(processor, payload(sequence=0, boot_id="boot-b"))

    assert result.status == "accepted"
    assert result.gap_size == 0
    assert sink.events == []


def test_out_of_order_message_is_kept_but_reported():
    sink = RecordingSink()
    processor = TelemetryProcessor(sink)
    process(processor, payload(sequence=5))

    result = process(processor, payload(sequence=3))

    assert result.status == "accepted"
    assert len(sink.telemetry) == 2
    assert sink.events[-1].event_type == "out_of_order"


def test_invalid_payload_becomes_pipeline_event():
    sink = RecordingSink()

    result = process(TelemetryProcessor(sink), payload(humidity_pct=150))

    assert result.status == "rejected"
    assert result.error_code == "out_of_range"
    assert sink.telemetry == []
    assert sink.events[0].event_type == "invalid_payload"
    assert sink.events[0].details["error_code"] == "out_of_range"


def test_topic_mismatch_is_rejected():
    sink = RecordingSink()

    result = process(
        TelemetryProcessor(sink),
        payload(),
        topic="iot/lab2/esp32-99/telemetry",
    )

    assert result.status == "rejected"
    assert result.error_code == "device_mismatch"


class FailsFirstTelemetryWrite(RecordingSink):
    def __init__(self):
        super().__init__()
        self.attempts = 0

    def write_telemetry(self, telemetry, *, received_at_ms, latency_ms):
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("temporary failure")
        super().write_telemetry(
            telemetry,
            received_at_ms=received_at_ms,
            latency_ms=latency_ms,
        )


def test_failed_write_can_be_retried_instead_of_becoming_duplicate():
    sink = FailsFirstTelemetryWrite()
    processor = TelemetryProcessor(sink)

    failed = process(processor, payload(sequence=20))
    retried = process(processor, payload(sequence=20))

    assert failed.status == "write_failed"
    assert retried.status == "accepted"
    assert sink.attempts == 2
    assert len(sink.telemetry) == 1
    assert sink.events[0].event_type == "write_error"


def test_sequence_tracker_uses_device_boot_and_sequence_as_duplicate_key():
    tracker = SequenceTracker(max_entries=10)
    first = Telemetry(1, "dev-a", "boot-a", 1, NOW_MS, 20, 50, 10)
    other_device = Telemetry(1, "dev-b", "boot-a", 1, NOW_MS, 20, 50, 10)
    other_boot = Telemetry(1, "dev-a", "boot-b", 1, NOW_MS, 20, 50, 10)

    tracker.record(first)

    assert tracker.inspect(first).duplicate is True
    assert tracker.inspect(other_device).duplicate is False
    assert tracker.inspect(other_boot).duplicate is False
