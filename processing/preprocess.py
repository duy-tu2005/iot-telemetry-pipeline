"""Clean, resample, enrich, and persist sensor data from InfluxDB.

The pure ``preprocess_dataframe`` function contains the data transformation
logic so it can be unit tested without an MQTT broker or InfluxDB instance.
The CLI queries the raw bucket and writes one-minute points to the processed
bucket. Re-running a time range is idempotent because InfluxDB identifies a
point by measurement, tag set, and timestamp.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


LOGGER = logging.getLogger("lab2.preprocessing")

PRIMARY_FIELDS = ("temperature_c", "humidity_pct", "distance_cm")
NUMERIC_FIELDS = PRIMARY_FIELDS + ("rssi_dbm", "latency_ms")
TAG_FIELDS = ("device_id", "location", "schema_version")
LOOKBACK_RE = re.compile(r"^-\d+[smhdw]$")
RESAMPLE_RE = re.compile(r"^(\d+)(ms|s|min|h|d|w)$")
RESAMPLE_SECONDS = {
    "ms": 0.001,
    "s": 1,
    "min": 60,
    "h": 3_600,
    "d": 86_400,
    "w": 604_800,
}


@dataclass(frozen=True)
class Settings:
    influx_url: str
    influx_token: str
    influx_org: str
    raw_bucket: str
    processed_bucket: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        values = {
            "influx_url": os.getenv("INFLUX_URL", "http://127.0.0.1:8086"),
            "influx_token": os.getenv("INFLUX_TOKEN", ""),
            "influx_org": os.getenv("INFLUX_ORG", "iot-lab"),
            "raw_bucket": os.getenv("INFLUX_RAW_BUCKET", "iot_raw"),
            "processed_bucket": os.getenv(
                "INFLUX_PROCESSED_BUCKET", "iot_processed"
            ),
        }
        if not values["influx_token"]:
            raise ValueError("INFLUX_TOKEN is required (set it in .env)")
        return cls(**values)


def _empty_result() -> pd.DataFrame:
    columns = [
        "_time",
        *TAG_FIELDS,
        *NUMERIC_FIELDS,
        "sample_count",
        "missing_count",
        "outlier_count",
        "quality_status",
    ]
    return pd.DataFrame(columns=columns)


def _normalise_input(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    data = frame.copy()
    if "_time" not in data.columns:
        if isinstance(data.index, pd.DatetimeIndex):
            data = data.reset_index().rename(columns={data.index.name or "index": "_time"})
        else:
            raise ValueError("Input data must contain an _time column")

    data["_time"] = pd.to_datetime(data["_time"], utc=True, errors="coerce")
    data = data.dropna(subset=["_time"])
    if "device_id" not in data.columns:
        raise ValueError("Input data must contain device_id")

    for column in NUMERIC_FIELDS:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")

    # An exact replay has the same source timestamp and message identity.
    identity = [column for column in ("device_id", "boot_id", "sequence", "_time") if column in data]
    if identity:
        data = data.drop_duplicates(subset=identity, keep="last")
    return data.sort_values(["device_id", "_time"])


def _iqr_mask(series: pd.Series, factor: float) -> pd.Series:
    values = series.dropna()
    if len(values) < 4:
        return pd.Series(False, index=series.index)
    q1 = values.quantile(0.25)
    q3 = values.quantile(0.75)
    iqr = q3 - q1
    if not math.isfinite(float(iqr)) or iqr == 0:
        # With a flat signal, any different value is a meaningful spike.
        return series.notna() & series.ne(q1)
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return series.notna() & ((series < lower) | (series > upper))


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean(skipna=True)
    std = series.std(skipna=True, ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.where(series.notna(), 0.0, np.nan), index=series.index)
    return (series - mean) / std


def _process_device(
    device: pd.DataFrame,
    *,
    resample_rule: str,
    expected_interval_seconds: int,
    iqr_factor: float,
    interpolation_limit: int,
) -> pd.DataFrame:
    device = device.set_index("_time").sort_index()
    available_numeric = [column for column in NUMERIC_FIELDS if column in device]
    if not available_numeric:
        return pd.DataFrame()

    outlier_flags = pd.DataFrame(False, index=device.index, columns=PRIMARY_FIELDS)
    cleaned = device.copy()
    for column in PRIMARY_FIELDS:
        if column not in cleaned:
            continue
        mask = _iqr_mask(cleaned[column], iqr_factor)
        outlier_flags.loc[:, column] = mask
        cleaned.loc[mask, column] = np.nan

    # Repair only short raw gaps. Long gaps remain visible after resampling.
    cleaned[available_numeric] = cleaned[available_numeric].interpolate(
        method="time", limit=interpolation_limit, limit_area="inside"
    )

    aggregates: dict[str, str] = {column: "mean" for column in available_numeric}
    resampled = cleaned[available_numeric].resample(resample_rule).agg(aggregates)
    sample_count = device[available_numeric[0]].resample(resample_rule).size()
    outlier_count = outlier_flags.sum(axis=1).resample(resample_rule).sum()

    match = RESAMPLE_RE.fullmatch(resample_rule)
    if not match:
        raise ValueError(f"Invalid resample rule: {resample_rule}")
    window_seconds = int(int(match.group(1)) * RESAMPLE_SECONDS[match.group(2)])
    if window_seconds < 1:
        raise ValueError("Resample rule must be at least one second")
    expected = max(1, window_seconds // expected_interval_seconds)

    resampled["sample_count"] = sample_count.astype("int64")
    resampled["missing_count"] = (expected - sample_count).clip(lower=0).astype("int64")
    resampled["outlier_count"] = outlier_count.astype("int64")

    resampled[available_numeric] = resampled[available_numeric].interpolate(
        method="time", limit=interpolation_limit, limit_area="inside"
    )

    for column in PRIMARY_FIELDS:
        if column not in resampled:
            continue
        resampled[f"{column}_rolling_5m"] = resampled[column].rolling(
            window=5, min_periods=1
        ).mean()
        resampled[f"{column}_delta"] = resampled[column].diff()
        resampled[f"{column}_zscore"] = _zscore(resampled[column])

    primary_present = [column for column in PRIMARY_FIELDS if column in resampled]
    still_missing = (
        resampled[primary_present].isna().any(axis=1)
        if primary_present
        else pd.Series(False, index=resampled.index)
    )
    resampled["quality_status"] = np.select(
        [still_missing, resampled["outlier_count"].gt(0), resampled["missing_count"].gt(0)],
        ["missing", "outlier_corrected", "interpolated"],
        default="ok",
    )

    for tag in TAG_FIELDS:
        if tag in device.columns and not device[tag].dropna().empty:
            resampled[tag] = str(device[tag].dropna().iloc[-1])
    resampled["device_id"] = str(device["device_id"].dropna().iloc[-1])
    return resampled.reset_index()


def preprocess_dataframe(
    frame: pd.DataFrame,
    *,
    resample_rule: str = "1min",
    expected_interval_seconds: int = 5,
    iqr_factor: float = 1.5,
    interpolation_limit: int = 2,
) -> pd.DataFrame:
    """Return cleaned one-minute sensor data for all devices in ``frame``."""

    data = _normalise_input(frame)
    if data.empty:
        return _empty_result()

    outputs: list[pd.DataFrame] = []
    for _, device in data.groupby("device_id", sort=False):
        output = _process_device(
            device,
            resample_rule=resample_rule,
            expected_interval_seconds=expected_interval_seconds,
            iqr_factor=iqr_factor,
            interpolation_limit=interpolation_limit,
        )
        if not output.empty:
            outputs.append(output)

    if not outputs:
        return _empty_result()
    return pd.concat(outputs, ignore_index=True).sort_values(["device_id", "_time"])


def _flux_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def query_raw(client: InfluxDBClient, settings: Settings, lookback: str) -> pd.DataFrame:
    if not LOOKBACK_RE.fullmatch(lookback):
        raise ValueError("lookback must look like -30m, -2h, or -7d")
    bucket = _flux_string(settings.raw_bucket)
    query = f'''
from(bucket: "{bucket}")
  |> range(start: {lookback})
  |> filter(fn: (r) => r._measurement == "sensor_raw")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
'''
    result = client.query_api().query_data_frame(query, org=settings.influx_org)
    if isinstance(result, list):
        frames = [item for item in result if isinstance(item, pd.DataFrame) and not item.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return result if isinstance(result, pd.DataFrame) else pd.DataFrame()


def _point_fields(row: pd.Series) -> Iterable[tuple[str, object]]:
    ignored = {"_time", *TAG_FIELDS, "result", "table", "_start", "_stop", "_measurement"}
    for name, value in row.items():
        if name in ignored or pd.isna(value):
            continue
        if isinstance(value, (np.bool_, bool)):
            yield name, bool(value)
        elif isinstance(value, (np.integer, int)):
            yield name, int(value)
        elif isinstance(value, (np.floating, float)):
            yield name, float(value)
        elif isinstance(value, str):
            yield name, value


def write_processed(
    client: InfluxDBClient, settings: Settings, frame: pd.DataFrame
) -> int:
    points: list[Point] = []
    for _, row in frame.iterrows():
        point = Point("sensor_processed_1m")
        for tag in TAG_FIELDS:
            if tag in row and pd.notna(row[tag]):
                point = point.tag(tag, str(row[tag]))
        fields = list(_point_fields(row))
        if not fields:
            continue
        for name, value in fields:
            point = point.field(name, value)
        point = point.time(pd.Timestamp(row["_time"]).to_pydatetime(), WritePrecision.NS)
        points.append(point)

    if points:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=settings.processed_bucket,
            org=settings.influx_org,
            record=points,
        )
    return len(points)


def run_once(settings: Settings, lookback: str) -> int:
    with InfluxDBClient(
        url=settings.influx_url,
        token=settings.influx_token,
        org=settings.influx_org,
        timeout=20_000,
    ) as client:
        raw = query_raw(client, settings, lookback)
        processed = preprocess_dataframe(raw)
        written = write_processed(client, settings, processed)
    LOGGER.info("raw_rows=%d processed_rows=%d written=%d", len(raw), len(processed), written)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback", default="-2h", help="Flux range, for example -2h")
    parser.add_argument("--watch", action="store_true", help="Repeat processing until stopped")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between watch runs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    if args.interval < 10:
        raise ValueError("--interval must be at least 10 seconds")

    while True:
        try:
            run_once(settings, args.lookback)
        except Exception:
            LOGGER.exception("Preprocessing run failed")
            if not args.watch:
                return 1
        if not args.watch:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
