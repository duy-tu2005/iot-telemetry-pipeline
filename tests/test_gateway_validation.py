import json

import pytest

from gateway.validation import TelemetryValidationError, parse_telemetry


NOW_MS = 1_800_000_000_000


def valid_payload(**overrides):
    payload = {
        "schema_version": 1,
        "device_id": "esp32-01",
        "boot_id": "A1B2C3D4",
        "sequence": 42,
        "sent_at_ms": NOW_MS - 750,
        "temperature_c": 25.4,
        "humidity_pct": 54.0,
        "distance_cm": 38.0,
        "rssi_dbm": -48,
        "uptime_s": 210,
        "led_on": False,
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


def test_valid_payload_is_parsed_to_typed_telemetry():
    telemetry = parse_telemetry(valid_payload(), now_ms=NOW_MS)

    assert telemetry.device_id == "esp32-01"
    assert telemetry.message_key == ("esp32-01", "A1B2C3D4", 42)
    assert telemetry.temperature_c == 25.4
    assert telemetry.led_on is False


def test_optional_diagnostic_fields_may_be_absent():
    decoded = json.loads(valid_payload())
    decoded.pop("rssi_dbm")
    decoded.pop("uptime_s")
    decoded.pop("led_on")

    telemetry = parse_telemetry(json.dumps(decoded), now_ms=NOW_MS)

    assert telemetry.rssi_dbm is None
    assert telemetry.uptime_s is None
    assert telemetry.led_on is None


@pytest.mark.parametrize(
    ("overrides", "error_code"),
    [
        ({"humidity_pct": 100.1}, "out_of_range"),
        ({"temperature_c": -40.1}, "out_of_range"),
        ({"distance_cm": 401}, "out_of_range"),
        ({"sequence": True}, "invalid_type"),
        ({"led_on": 1}, "invalid_type"),
        ({"sent_at_ms": NOW_MS + 300_001}, "future_timestamp"),
        ({"device_id": "bad device"}, "invalid_value"),
    ],
)
def test_invalid_values_are_rejected(overrides, error_code):
    with pytest.raises(TelemetryValidationError) as raised:
        parse_telemetry(valid_payload(**overrides), now_ms=NOW_MS)

    assert raised.value.code == error_code


def test_non_standard_nan_is_rejected_even_though_python_json_accepts_it():
    payload = valid_payload().decode().replace("25.4", "NaN")

    with pytest.raises(TelemetryValidationError) as raised:
        parse_telemetry(payload, now_ms=NOW_MS)

    assert raised.value.code == "invalid_json"


def test_missing_required_field_identifies_device():
    decoded = json.loads(valid_payload())
    decoded.pop("temperature_c")

    with pytest.raises(TelemetryValidationError) as raised:
        parse_telemetry(json.dumps(decoded), now_ms=NOW_MS)

    assert raised.value.code == "missing_field"
    assert raised.value.device_id == "esp32-01"


def test_device_id_must_match_mqtt_topic_device_segment():
    with pytest.raises(TelemetryValidationError) as raised:
        parse_telemetry(
            valid_payload(),
            now_ms=NOW_MS,
            expected_device_id="esp32-02",
        )

    assert raised.value.code == "device_mismatch"


@pytest.mark.parametrize("payload", [b"not json", b"[]", b"\xff"])
def test_malformed_payload_is_rejected(payload):
    with pytest.raises(TelemetryValidationError):
        parse_telemetry(payload, now_ms=NOW_MS)
