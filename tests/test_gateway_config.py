import pytest

from gateway.config import ConfigurationError, Settings


def test_expected_defaults_and_required_token():
    settings = Settings.from_env({"INFLUX_TOKEN": "secret"})

    assert settings.mqtt_host == "127.0.0.1"
    assert settings.mqtt_port == 1884
    assert settings.mqtt_topic == "iot/lab2/+/telemetry"
    assert settings.influx_url == "http://127.0.0.1:8086"
    assert settings.influx_org == "iot-lab"
    assert settings.influx_raw_bucket == "iot_raw"
    assert settings.location == "lab"


def test_missing_influx_token_fails_fast():
    with pytest.raises(ConfigurationError, match="INFLUX_TOKEN"):
        Settings.from_env({})


@pytest.mark.parametrize(
    "environment",
    [
        {"INFLUX_TOKEN": "x", "MQTT_PORT": "zero"},
        {"INFLUX_TOKEN": "x", "MQTT_PORT": "70000"},
        {"INFLUX_TOKEN": "x", "MQTT_QOS": "3"},
        {"INFLUX_TOKEN": "x", "MQTT_PASSWORD": "secret"},
        {"INFLUX_TOKEN": "x", "GATEWAY_LOG_LEVEL": "LOUD"},
    ],
)
def test_invalid_environment_is_rejected(environment):
    with pytest.raises(ConfigurationError):
        Settings.from_env(environment)
