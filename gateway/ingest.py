"""Executable MQTT subscriber for the IoT laboratory pipeline."""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from typing import Any

from .config import ConfigurationError, Settings
from .influx_sink import InfluxSink
from .processor import PipelineEvent, SequenceTracker, TelemetryProcessor


LOGGER = logging.getLogger(__name__)


class MQTTGateway:
    def __init__(self, settings: Settings, sink: InfluxSink):
        import paho.mqtt.client as mqtt

        self._mqtt_module = mqtt
        self._settings = settings
        self._sink = sink
        self._processor = TelemetryProcessor(
            sink,
            tracker=SequenceTracker(settings.dedupe_cache_size),
        )
        self._stopping = threading.Event()
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            clean_session=False,
            protocol=mqtt.MQTTv311,
        )
        if settings.mqtt_username is not None:
            self._client.username_pw_set(
                settings.mqtt_username, settings.mqtt_password
            )
        self._client.reconnect_delay_set(min_delay=1, max_delay=120)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        connect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if reason_code.is_failure:
            LOGGER.error("MQTT connection rejected: %s", reason_code)
            return
        result, message_id = client.subscribe(
            self._settings.mqtt_topic, qos=self._settings.mqtt_qos
        )
        if result != self._mqtt_module.MQTT_ERR_SUCCESS:
            LOGGER.error("MQTT subscribe failed with result code %s", result)
            return
        LOGGER.info(
            "Connected to MQTT and subscribed topic=%s qos=%d mid=%d",
            self._settings.mqtt_topic,
            self._settings.mqtt_qos,
            message_id,
        )

    def _on_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if self._stopping.is_set():
            LOGGER.info("MQTT client disconnected")
        else:
            LOGGER.warning(
                "MQTT disconnected (%s); automatic reconnect is enabled", reason_code
            )

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        self._processor.process(
            message.payload,
            topic=message.topic,
            received_at_ms=int(time.time() * 1000),
        )

    def run(self) -> None:
        LOGGER.info(
            "Starting gateway mqtt=%s:%d influx=%s bucket=%s",
            self._settings.mqtt_host,
            self._settings.mqtt_port,
            self._settings.influx_url,
            self._settings.influx_raw_bucket,
        )
        self._client.connect_async(
            self._settings.mqtt_host,
            self._settings.mqtt_port,
            keepalive=60,
        )
        # Paho retries the first connection as well as later reconnects. Its
        # exponential delay is configured in __init__.
        self._client.loop_forever(retry_first_connection=True)

    def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        try:
            self._sink.write_event(
                PipelineEvent(
                    event_type="gateway_stopped",
                    severity="info",
                    received_at_ms=int(time.time() * 1000),
                    message="Gateway stopped cleanly",
                )
            )
        except Exception:
            LOGGER.exception("Could not persist shutdown event")
        self._client.disconnect()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
        settings = Settings.from_env()
    except (ConfigurationError, ImportError) as exc:
        print(f"Gateway configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level)
    sink: InfluxSink | None = None
    gateway: MQTTGateway | None = None
    try:
        sink = InfluxSink(
            url=settings.influx_url,
            token=settings.influx_token,
            org=settings.influx_org,
            bucket=settings.influx_raw_bucket,
            location=settings.location,
        )
        gateway = MQTTGateway(settings, sink)

        def request_stop(signum: int, frame: Any) -> None:
            LOGGER.info("Received signal %d; stopping", signum)
            assert gateway is not None
            gateway.stop()

        signal.signal(signal.SIGINT, request_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, request_stop)
        gateway.run()
        return 0
    except KeyboardInterrupt:
        if gateway is not None:
            gateway.stop()
        return 0
    except Exception:
        LOGGER.exception("Gateway stopped because of an unrecoverable error")
        return 1
    finally:
        if sink is not None:
            sink.close()


if __name__ == "__main__":
    raise SystemExit(main())
