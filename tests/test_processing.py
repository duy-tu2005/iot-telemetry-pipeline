from __future__ import annotations

import numpy as np
import pandas as pd

from processing.preprocess import preprocess_dataframe


def make_raw(minutes: int = 10) -> pd.DataFrame:
    times = pd.date_range("2026-09-29T03:00:00Z", periods=minutes * 12, freq="5s")
    temperature = np.full(len(times), 25.0)
    temperature[30] = 250.0
    return pd.DataFrame(
        {
            "_time": times,
            "device_id": "esp32-01",
            "location": "wokwi-lab",
            "schema_version": "1",
            "boot_id": "boot-a",
            "sequence": np.arange(1, len(times) + 1),
            "temperature_c": temperature,
            "humidity_pct": np.linspace(50.0, 55.0, len(times)),
            "distance_cm": np.linspace(90.0, 100.0, len(times)),
            "rssi_dbm": -45,
            "latency_ms": 25,
        }
    )


def test_preprocess_resamples_and_creates_features() -> None:
    result = preprocess_dataframe(make_raw())

    assert len(result) == 10
    assert result["sample_count"].eq(12).all()
    assert result["missing_count"].eq(0).all()
    assert result["outlier_count"].sum() == 1
    assert "temperature_c_rolling_5m" in result
    assert "temperature_c_delta" in result
    assert "temperature_c_zscore" in result
    assert result["temperature_c"].max() < 30
    assert set(result["device_id"]) == {"esp32-01"}


def test_preprocess_detects_missing_samples_and_exact_replay() -> None:
    raw = make_raw(3)
    raw = raw.drop(index=[14, 15])
    raw = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)

    result = preprocess_dataframe(raw)

    assert len(result) == 3
    assert result["sample_count"].sum() == 34
    assert result["missing_count"].sum() == 2
    assert "interpolated" in set(result["quality_status"])


def test_preprocess_empty_input() -> None:
    result = preprocess_dataframe(pd.DataFrame())
    assert result.empty
    assert "_time" in result.columns
