"""Tests for the Open-Meteo client.

Every response is a recorded fixture — no test in this file touches the network,
so CI stays offline-safe and the suite does not depend on a third party being up.

The behaviour worth testing here is the resilience, not the happy path: a 429
that is not retried loses a farm's weather silently, and a 400 that *is* retried
burns the quota the rate limiter exists to protect.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import polars as pl
import pytest
import requests
import responses

from smart_agri.config import OpenMeteoSettings
from smart_agri.io.openmeteo import (
    DAILY_VARIABLES,
    OpenMeteoError,
    OpenMeteoResponse,
    OpenMeteoSource,
    RateLimiter,
    RetryableOpenMeteoError,
    WeatherSource,
    WeatherWindow,
)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def recorded_response(days: int = 3, *, latitude: float = 30.9, longitude: float = 31.1) -> dict:
    """A response shaped exactly like Open-Meteo's, with plausible values."""
    dates = [f"2026-07-{day:02d}" for day in range(1, days + 1)]
    daily: dict[str, Any] = {"time": dates}
    series: dict[str, list[Any]] = {
        "temperature_2m_max": [34.2, 35.8, 33.1],
        "temperature_2m_min": [21.4, 22.0, 20.8],
        "temperature_2m_mean": [27.6, 28.5, 26.7],
        "precipitation_sum": [0.0, 12.4, 3.2],
        "rain_sum": [0.0, 12.4, 3.2],
        "precipitation_hours": [0.0, 4.0, 2.0],
        "et0_fao_evapotranspiration": [6.1, 4.2, 5.4],
        "shortwave_radiation_sum": [27.3, 19.8, 24.1],
        "wind_speed_10m_max": [18.4, 22.1, 15.9],
        "weather_code": [0, 61, 51],
    }
    for name, values in series.items():
        daily[name] = values[:days]

    return {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": "GMT",
        "utc_offset_seconds": 0,
        "daily_units": {name: "" for name in DAILY_VARIABLES},
        "daily": daily,
    }


@pytest.fixture
def settings() -> OpenMeteoSettings:
    # A high rate cap so the limiter does not slow the suite down; the limiter's
    # own behaviour is tested directly.
    return OpenMeteoSettings(max_rps=1000.0, max_retries=3, timeout_s=5.0)


@pytest.fixture
def source(settings: OpenMeteoSettings) -> OpenMeteoSource:
    return OpenMeteoSource(settings)


ARCHIVE_WINDOW = WeatherWindow(date(2026, 7, 1), date(2026, 7, 3), WeatherSource.ARCHIVE)


# --- response parsing --------------------------------------------------------
class TestResponseParsing:
    def test_parallel_arrays_become_one_row_per_day(self) -> None:
        parsed = OpenMeteoResponse.model_validate(recorded_response(days=3))
        frame = parsed.to_frame("EG-001", WeatherSource.ARCHIVE)

        assert frame.height == 3
        assert frame["weather_date"].to_list() == [
            date(2026, 7, 1),
            date(2026, 7, 2),
            date(2026, 7, 3),
        ]
        assert set(frame["farm_code"].to_list()) == {"EG-001"}
        assert set(frame["source"].to_list()) == {"archive"}

    def test_values_land_on_the_right_dates(self) -> None:
        frame = OpenMeteoResponse.model_validate(recorded_response(days=3)).to_frame(
            "EG-001", WeatherSource.ARCHIVE
        )
        rainy = frame.filter(pl.col("weather_date") == date(2026, 7, 2))
        assert rainy["precipitation_sum"].item() == pytest.approx(12.4)
        assert rainy["et0_fao_evapotranspiration"].item() == pytest.approx(4.2)

    def test_misaligned_arrays_are_rejected(self) -> None:
        """The arrays are positional, so a short one would shift every value
        after the gap onto the wrong date — silently, and forever."""
        payload = recorded_response(days=3)
        payload["daily"]["precipitation_sum"] = [1.0, 2.0]

        with pytest.raises(ValueError, match="misaligned"):
            OpenMeteoResponse.model_validate(payload)

    def test_missing_variables_become_nulls(self) -> None:
        payload = recorded_response(days=2)
        del payload["daily"]["wind_speed_10m_max"]

        frame = OpenMeteoResponse.model_validate(payload).to_frame("EG-001", WeatherSource.FORECAST)
        assert frame["wind_speed_10m_max"].null_count() == 2

    def test_null_values_survive_parsing(self) -> None:
        """A gap in the record is a gap, not a zero."""
        payload = recorded_response(days=3)
        payload["daily"]["precipitation_sum"] = [0.0, None, 3.2]

        frame = OpenMeteoResponse.model_validate(payload).to_frame("EG-001", WeatherSource.ARCHIVE)
        assert frame["precipitation_sum"].null_count() == 1

    def test_schema_is_stable(self) -> None:
        frame = OpenMeteoResponse.model_validate(recorded_response()).to_frame(
            "EG-001", WeatherSource.ARCHIVE
        )
        assert frame["weather_date"].dtype == pl.Date
        assert frame["weather_code"].dtype == pl.Int32


# --- window planning ---------------------------------------------------------
class TestWindowPlanning:
    def test_archive_window_stops_short_of_the_logical_date(
        self, source: OpenMeteoSource, settings: OpenMeteoSettings
    ) -> None:
        """The archive lags real time; requesting up to today returns a gap."""
        window = source.archive_window(date(2025, 8, 1), date(2026, 8, 6))
        assert window.start == date(2025, 8, 1)
        assert (date(2026, 8, 6) - window.end).days == settings.archive_lag_days
        assert window.source is WeatherSource.ARCHIVE

    def test_forecast_window_straddles_the_logical_date(self, source: OpenMeteoSource) -> None:
        window = source.forecast_window(date(2026, 8, 6))
        assert window.start < date(2026, 8, 6) < window.end
        assert window.source is WeatherSource.FORECAST

    def test_the_two_windows_overlap(self, source: OpenMeteoSource) -> None:
        """No gap between where the archive stops and the forecast starts —
        otherwise the series would have a hole a few days wide, permanently."""
        logical = date(2026, 8, 6)
        archive = source.archive_window(date(2025, 8, 1), logical)
        forecast = source.forecast_window(logical)
        assert forecast.start <= archive.end

    def test_a_backwards_window_is_empty(self, source: OpenMeteoSource) -> None:
        assert source.archive_window(date(2026, 8, 20), date(2026, 8, 6)).is_empty

    @responses.activate
    def test_an_empty_window_makes_no_request(self, source: OpenMeteoSource) -> None:
        empty = WeatherWindow(date(2026, 8, 20), date(2026, 8, 1), WeatherSource.ARCHIVE)
        frame = source.fetch_many([("EG-001", 30.9, 31.1)], empty)

        assert frame.is_empty()
        assert len(responses.calls) == 0


# --- fetching ----------------------------------------------------------------
class TestFetching:
    @responses.activate
    def test_archive_request_carries_the_date_range(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)
        source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)

        query = responses.calls[0].request.params
        assert query["start_date"] == "2026-07-01"
        assert query["end_date"] == "2026-07-03"
        assert query["timezone"] == "UTC"

    @responses.activate
    def test_forecast_request_uses_relative_days(
        self, source: OpenMeteoSource, settings: OpenMeteoSettings
    ) -> None:
        """The forecast endpoint has no date range; it takes day counts."""
        responses.add(responses.GET, FORECAST_URL, json=recorded_response(), status=200)
        window = source.forecast_window(date(2026, 8, 6))
        source.fetch("EG-001", 30.9, 31.1, window)

        query = responses.calls[0].request.params
        assert query["past_days"] == str(settings.forecast_past_days)
        assert query["forecast_days"] == str(settings.forecast_days)
        assert "start_date" not in query

    @responses.activate
    def test_every_daily_variable_is_requested(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)
        source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)

        requested = responses.calls[0].request.params["daily"].split(",")
        assert set(requested) == set(DAILY_VARIABLES)

    @responses.activate
    def test_many_farms_are_concatenated(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)
        farms = [("EG-001", 30.9, 31.1), ("NG-001", 11.7, 8.6), ("MA-001", 34.4, -6.1)]

        frame = source.fetch_many(farms, ARCHIVE_WINDOW)
        assert frame.height == 9
        assert set(frame["farm_code"].to_list()) == {"EG-001", "NG-001", "MA-001"}


# --- resilience --------------------------------------------------------------
class TestRetryBehaviour:
    @responses.activate
    def test_throttling_is_retried_and_then_succeeds(self, source: OpenMeteoSource) -> None:
        """A 429 that is not retried loses that farm's weather silently — the
        pipeline would succeed with a gap rather than fail."""
        responses.add(responses.GET, ARCHIVE_URL, json={}, status=429)
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)

        frame = source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)
        assert frame.height == 3
        assert len(responses.calls) == 2

    @responses.activate
    def test_server_errors_are_retried(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json={}, status=503)
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)

        assert source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW).height == 3

    @responses.activate
    def test_a_bad_request_is_not_retried(self, source: OpenMeteoSource) -> None:
        """A 400 will stay 400; retrying it only burns the quota the rate
        limiter exists to protect."""
        responses.add(responses.GET, ARCHIVE_URL, body="bad latitude", status=400)

        with pytest.raises(OpenMeteoError, match="400"):
            source.fetch("EG-001", 999.0, 31.1, ARCHIVE_WINDOW)
        assert len(responses.calls) == 1

    @responses.activate
    def test_retries_are_bounded(self, settings: OpenMeteoSettings) -> None:
        source = OpenMeteoSource(settings.model_copy(update={"max_retries": 2}))
        responses.add(responses.GET, ARCHIVE_URL, json={}, status=429)

        with pytest.raises(RetryableOpenMeteoError):
            source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)
        assert len(responses.calls) == 3  # the attempt plus two retries

    @responses.activate
    def test_connection_errors_are_retried(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, body=requests.exceptions.ConnectionError())
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)

        assert source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW).height == 3

    @responses.activate
    def test_an_error_body_with_a_200_is_still_an_error(self, source: OpenMeteoSource) -> None:
        """Open-Meteo reports some failures in the body with an HTTP 200; taking
        the status code at face value would parse the error as weather."""
        responses.add(
            responses.GET,
            ARCHIVE_URL,
            json={"error": True, "reason": "Value out of allowed range"},
            status=200,
        )
        with pytest.raises(OpenMeteoError, match="out of allowed range"):
            source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)


class TestCaching:
    @responses.activate
    def test_an_identical_window_is_fetched_once(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)

        first = source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)
        second = source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)

        assert first.equals(second)
        assert len(responses.calls) == 1

    @responses.activate
    def test_a_different_farm_is_fetched_again(self, source: OpenMeteoSource) -> None:
        responses.add(responses.GET, ARCHIVE_URL, json=recorded_response(), status=200)

        source.fetch("EG-001", 30.9, 31.1, ARCHIVE_WINDOW)
        source.fetch("NG-001", 11.7, 8.6, ARCHIVE_WINDOW)
        assert len(responses.calls) == 2


class TestRateLimiter:
    def test_calls_are_spaced_out(self) -> None:
        limiter = RateLimiter(max_rps=50.0)  # 20 ms apart
        started = time.monotonic()
        for _ in range(3):
            limiter.wait()
        elapsed = time.monotonic() - started

        # Two gaps between three calls.
        assert elapsed >= 0.04

    def test_the_first_call_is_not_delayed(self) -> None:
        limiter = RateLimiter(max_rps=1.0)
        started = time.monotonic()
        limiter.wait()
        assert time.monotonic() - started < 0.1
