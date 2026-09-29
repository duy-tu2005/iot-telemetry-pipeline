from gateway.influx_sink import InfluxSink
from gateway.processor import PipelineEvent
from gateway.validation import Telemetry


class RecordingWriteApi:
    def __init__(self):
        self.calls = []

    def write(self, **kwargs):
        self.calls.append(kwargs)


def make_sink():
    # Bypass the network client constructor while exercising the real Point
    # construction performed by the sink.
    sink = InfluxSink.__new__(InfluxSink)
    sink._org = "iot-lab"
    sink._bucket = "iot_raw"
    sink._location = "lab"
    sink._write_api = RecordingWriteApi()
    return sink


def test_raw_point_contains_expected_measurement_tags_fields_and_time():
    sink = make_sink()
    telemetry = Telemetry(
        schema_version=1,
        device_id="esp32-01",
        boot_id="boot-a",
        sequence=4,
        sent_at_ms=1_800_000_000_000,
        temperature_c=25.5,
        humidity_pct=50.0,
        distance_cm=80.0,
        rssi_dbm=-50,
        uptime_s=20,
        led_on=True,
    )

    sink.write_telemetry(
        telemetry,
        received_at_ms=1_800_000_000_025,
        latency_ms=25,
    )

    call = sink._write_api.calls[0]
    line = call["record"].to_line_protocol()
    assert call["bucket"] == "iot_raw"
    assert call["org"] == "iot-lab"
    assert line.startswith(
        "sensor_raw,device_id=esp32-01,location=lab,schema_version=1 "
    )
    assert 'boot_id="boot-a"' in line
    assert "sequence=4i" in line
    assert "latency_ms=25i" in line
    assert "temperature_c=25.5" in line
    assert "led_on=true" in line
    assert line.endswith("1800000000000")


def test_pipeline_event_is_written_to_same_bucket():
    sink = make_sink()
    event = PipelineEvent(
        event_type="invalid_payload",
        severity="error",
        received_at_ms=1_800_000_000_100,
        message="humidity_pct must be between 0 and 100",
        device_id="esp32-01",
        details={"error_code": "out_of_range"},
    )

    sink.write_event(event)

    call = sink._write_api.calls[0]
    line = call["record"].to_line_protocol()
    assert line.startswith(
        "pipeline_event,device_id=esp32-01,event_type=invalid_payload,"
        "location=lab,severity=error "
    )
    assert 'error_code="out_of_range"' in line
    assert line.endswith("1800000000100")
