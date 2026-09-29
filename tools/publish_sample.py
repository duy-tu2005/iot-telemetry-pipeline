"""Publish deterministic sample telemetry for end-to-end smoke tests."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument(
        "--include-anomalies",
        action="store_true",
        help="Also send one duplicate, one sequence gap, and one invalid payload",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1 or args.interval < 0:
        raise ValueError("--count must be positive and --interval cannot be negative")

    load_dotenv()
    host = os.getenv("MQTT_HOST", "127.0.0.1")
    port = int(os.getenv("MQTT_PORT", "1884"))
    device_id = "esp32-01"
    topic = f"iot/lab2/{device_id}/telemetry"
    boot_id = f"smoke-{uuid.uuid4().hex[:8]}"

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"lab2-smoke-{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv311,
    )
    username = os.getenv("MQTT_USERNAME", "").strip()
    if username:
        client.username_pw_set(username, os.getenv("MQTT_PASSWORD"))
    client.connect(host, port, keepalive=30)
    client.loop_start()

    first_payload: str | None = None
    try:
        for index in range(args.count):
            sequence = index + 1
            if args.include_anomalies and index >= 4:
                sequence += 1  # Sequence 5 is intentionally absent.
            payload = json.dumps(
                {
                    "schema_version": 1,
                    "device_id": device_id,
                    "boot_id": boot_id,
                    "sequence": sequence,
                    "sent_at_ms": int(time.time() * 1000),
                    "temperature_c": round(24.0 + index * 0.05, 2),
                    "humidity_pct": round(52.0 + index * 0.1, 2),
                    "distance_cm": round(100.0 - index * 0.2, 2),
                    "rssi_dbm": -45,
                    "uptime_s": index * 5,
                    "led_on": False,
                },
                separators=(",", ":"),
            )
            first_payload = first_payload or payload
            info = client.publish(topic, payload, qos=1)
            info.wait_for_publish(timeout=5)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with code {info.rc}")
            print(f"published sequence={sequence}")
            time.sleep(args.interval)

        if args.include_anomalies and first_payload is not None:
            client.publish(topic, first_payload, qos=1).wait_for_publish(timeout=5)
            invalid = json.loads(first_payload)
            invalid["sequence"] = args.count + 10
            invalid["sent_at_ms"] = int(time.time() * 1000)
            invalid["humidity_pct"] = 150
            client.publish(topic, json.dumps(invalid), qos=1).wait_for_publish(timeout=5)
            print("published duplicate and invalid humidity payload")
    finally:
        client.disconnect()
        client.loop_stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
