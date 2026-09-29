"""Seed varied historical telemetry so Grafana charts are useful immediately."""

from __future__ import annotations

import argparse
import math
import os
import random
import time

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=90)
    parser.add_argument("--interval", type=int, default=5, help="Sample period in seconds")
    parser.add_argument("--device-id", default="esp32-01")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.minutes < 5:
        raise ValueError("--minutes must be at least 5")
    if args.interval < 1:
        raise ValueError("--interval must be positive")

    load_dotenv()
    url = os.getenv("INFLUX_URL", "http://127.0.0.1:8086")
    token = os.getenv("INFLUX_TOKEN", "")
    org = os.getenv("INFLUX_ORG", "iot-lab")
    bucket = os.getenv("INFLUX_RAW_BUCKET", "iot_raw")
    location = os.getenv("LOCATION", "wokwi-lab")
    if not token:
        raise ValueError("INFLUX_TOKEN is required in .env")

    sample_count = args.minutes * 60 // args.interval
    end_ms = int(time.time() // args.interval * args.interval * 1000)
    start_ms = end_ms - (sample_count - 1) * args.interval * 1000
    boot_id = f"demo-{start_ms}"
    rng = random.Random(2026)
    points: list[Point] = []

    for index in range(sample_count):
        # Deliberately omit occasional samples so preprocessing can count gaps.
        if index and index % 113 == 0:
            continue

        slow = 2 * math.pi * index / 360
        fast = 2 * math.pi * index / 41
        temperature = 25.0 + 3.2 * math.sin(slow) + 0.35 * math.sin(fast)
        humidity = 58.0 - 9.0 * math.sin(slow) + 1.8 * math.sin(fast / 2)
        distance = 120.0 + 52.0 * math.sin(2 * math.pi * index / 180)

        temperature += rng.uniform(-0.16, 0.16)
        humidity += rng.uniform(-0.7, 0.7)
        distance += rng.uniform(-2.5, 2.5)

        # Valid but obvious spikes exercise the IQR outlier correction.
        if index and index % 211 == 0:
            temperature += 12.0
            humidity += 24.0
            distance += 145.0

        latency_ms = rng.randint(8, 85)
        sent_at_ms = start_ms + index * args.interval * 1000
        point = (
            Point("sensor_raw")
            .tag("device_id", args.device_id)
            .tag("location", location)
            .tag("schema_version", "1")
            .field("boot_id", boot_id)
            .field("sequence", index + 1)
            .field("sent_at_ms", sent_at_ms)
            .field("received_at_ms", sent_at_ms + latency_ms)
            .field("latency_ms", latency_ms)
            .field("temperature_c", round(temperature, 2))
            .field("humidity_pct", round(max(0.0, min(100.0, humidity)), 2))
            .field("distance_cm", round(max(2.0, min(400.0, distance)), 2))
            .field("rssi_dbm", int(round(-52 + 6 * math.sin(fast) + rng.uniform(-2, 2))))
            .field("uptime_s", index * args.interval)
            .field("led_on", distance < 80)
            .time(sent_at_ms, WritePrecision.MS)
        )
        points.append(point)

    with InfluxDBClient(url=url, token=token, org=org, timeout=60_000) as client:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket,
            org=org,
            record=points,
        )

    print(
        f"seeded={len(points)} skipped={sample_count - len(points)} "
        f"minutes={args.minutes} bucket={bucket}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
