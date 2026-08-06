"""The Open-Meteo weather API.

This is the platform's only external dependency at runtime, and the only place
a pipeline can fail for reasons outside the stack. It is therefore the one
connector that needs real resilience:

* **Retry with exponential backoff** on throttling and transient server errors.
  A 429 that is not retried loses that farm's weather silently — the pipeline
  would succeed with a gap rather than fail loudly.
* **Client-side rate limiting.** The free tier throttles aggressively, and a
  backfill across fifty farms will trip it within seconds otherwise.
* **Response caching.** A backfill and a daily run often ask for overlapping
  windows; re-requesting them wastes the quota that the rate limiter is trying
  to protect.

Two endpoints are used because neither covers the whole timeline: the archive
lags real time by several days, and the forecast endpoint reaches only a few
weeks back. `WeatherWindow` decides which one a given date range needs.
"""

from __future__ import annotations

import time as time_module
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import polars as pl
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from smart_agri.config import OpenMeteoSettings

logger = get_logger(__name__)

#: Daily variables requested from both endpoints. Kept identical so archive and
#: forecast rows are union-compatible without reshaping.
DAILY_VARIABLES: tuple[str, ...] = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "et0_fao_evapotranspiration",
    "shortwave_radiation_sum",
    "wind_speed_10m_max",
    "weather_code",
)

WEATHER_SCHEMA: dict[str, pl.DataType] = {
    "farm_code": pl.String(),
    "latitude": pl.Float64(),
    "longitude": pl.Float64(),
    "weather_date": pl.Date(),
    "source": pl.String(),
    "temperature_2m_max": pl.Float64(),
    "temperature_2m_min": pl.Float64(),
    "temperature_2m_mean": pl.Float64(),
    "precipitation_sum": pl.Float64(),
    "rain_sum": pl.Float64(),
    "precipitation_hours": pl.Float64(),
    "et0_fao_evapotranspiration": pl.Float64(),
    "shortwave_radiation_sum": pl.Float64(),
    "wind_speed_10m_max": pl.Float64(),
    "weather_code": pl.Int32(),
}


class WeatherSource(StrEnum):
    """Which endpoint a row came from.

    Carried through to Silver because it decides precedence: an archive row is a
    measurement, a forecast row is a prediction, and where both exist for a date
    the measurement must win.
    """

    ARCHIVE = "archive"
    FORECAST = "forecast"


class OpenMeteoError(RuntimeError):
    """The API returned something unusable."""


class RetryableOpenMeteoError(OpenMeteoError):
    """Throttling or a transient server fault — worth trying again."""


# --- response models ---------------------------------------------------------
class DailyBlock(BaseModel):
    """The `daily` object: parallel arrays, one entry per day."""

    model_config = ConfigDict(extra="allow")

    time: list[date]
    temperature_2m_max: list[float | None] = Field(default_factory=list)
    temperature_2m_min: list[float | None] = Field(default_factory=list)
    temperature_2m_mean: list[float | None] = Field(default_factory=list)
    precipitation_sum: list[float | None] = Field(default_factory=list)
    rain_sum: list[float | None] = Field(default_factory=list)
    precipitation_hours: list[float | None] = Field(default_factory=list)
    et0_fao_evapotranspiration: list[float | None] = Field(default_factory=list)
    shortwave_radiation_sum: list[float | None] = Field(default_factory=list)
    wind_speed_10m_max: list[float | None] = Field(default_factory=list)
    weather_code: list[int | None] = Field(default_factory=list)

    @model_validator(mode="after")
    def _arrays_are_aligned(self) -> DailyBlock:
        """Every series must match the length of `time`.

        The arrays are positional, so a short one would silently shift every
        value after the gap onto the wrong date.
        """
        expected = len(self.time)
        for name in DAILY_VARIABLES:
            series = getattr(self, name, None)
            if series and len(series) != expected:
                msg = (
                    f"daily.{name} has {len(series)} values for {expected} dates; "
                    "the response arrays are misaligned"
                )
                raise ValueError(msg)
        return self


class OpenMeteoResponse(BaseModel):
    """A single API response for one location."""

    model_config = ConfigDict(extra="allow")

    latitude: float
    longitude: float
    timezone: str = "GMT"
    daily: DailyBlock

    def to_frame(self, farm_code: str, source: WeatherSource) -> pl.DataFrame:
        """Flatten the parallel arrays into one row per day."""
        rows: list[dict[str, Any]] = []
        for index, day in enumerate(self.daily.time):
            row: dict[str, Any] = {
                "farm_code": farm_code,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "weather_date": day,
                "source": source.value,
            }
            for name in DAILY_VARIABLES:
                series = getattr(self.daily, name, None)
                row[name] = series[index] if series else None
            rows.append(row)

        return pl.DataFrame(rows, schema=WEATHER_SCHEMA)


@dataclass(frozen=True, slots=True)
class WeatherWindow:
    """A date range, and which endpoint can serve it."""

    start: date
    end: date
    source: WeatherSource

    @property
    def is_empty(self) -> bool:
        return self.end < self.start


class RateLimiter:
    """Simple client-side throttle.

    A monotonic clock is used deliberately: the wall clock can step backwards
    over an NTP correction, which would make the limiter wait for hours.
    """

    def __init__(self, max_rps: float) -> None:
        self._min_interval = 1.0 / max_rps
        self._last_call: float | None = None

    def wait(self) -> None:
        now = time_module.monotonic()
        if self._last_call is not None:
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time_module.sleep(self._min_interval - elapsed)
        self._last_call = time_module.monotonic()


class OpenMeteoSource:
    """Fetches daily weather for a set of farms."""

    def __init__(
        self,
        settings: OpenMeteoSettings,
        session: requests.Session | None = None,
    ) -> None:
        self._settings = settings
        self._session = session or requests.Session()
        self._limiter = RateLimiter(settings.max_rps)
        # Keyed on the full request, so an identical window is served once per
        # process however many pipelines ask for it.
        self._cache: dict[tuple[Any, ...], OpenMeteoResponse] = {}

    # -- window planning -----------------------------------------------------
    def archive_window(self, start: date, logical_date: date) -> WeatherWindow:
        """The portion of a range the archive endpoint can serve.

        The archive lags real time, so the window stops short of the logical
        date; anything more recent belongs to the forecast endpoint.
        """
        end = logical_date - timedelta(days=self._settings.archive_lag_days)
        return WeatherWindow(start=start, end=end, source=WeatherSource.ARCHIVE)

    def forecast_window(self, logical_date: date) -> WeatherWindow:
        """The range around the logical date the forecast endpoint covers."""
        return WeatherWindow(
            start=logical_date - timedelta(days=self._settings.forecast_past_days),
            end=logical_date + timedelta(days=self._settings.forecast_days),
            source=WeatherSource.FORECAST,
        )

    # -- fetching ------------------------------------------------------------
    def fetch_many(
        self, farms: Sequence[tuple[str, float, float]], window: WeatherWindow
    ) -> pl.DataFrame:
        """Fetch one window for many farms and concatenate the result.

        Args:
            farms: `(farm_code, latitude, longitude)` triples.
            window: The range and endpoint to use.

        Returns an empty, correctly-typed frame when the window is empty, so a
        caller never has to special-case "no weather to fetch".
        """
        if window.is_empty:
            logger.info("weather_window_empty", start=str(window.start), end=str(window.end))
            return pl.DataFrame(schema=WEATHER_SCHEMA)

        frames = [
            self.fetch(farm_code, latitude, longitude, window)
            for farm_code, latitude, longitude in farms
        ]
        if not frames:
            return pl.DataFrame(schema=WEATHER_SCHEMA)

        result = pl.concat(frames, how="vertical")
        logger.info(
            "weather_fetched",
            farms=len(farms),
            rows=result.height,
            source=window.source.value,
            start=str(window.start),
            end=str(window.end),
        )
        return result

    def fetch(
        self, farm_code: str, latitude: float, longitude: float, window: WeatherWindow
    ) -> pl.DataFrame:
        """Fetch one window for one farm."""
        if window.is_empty:
            return pl.DataFrame(schema=WEATHER_SCHEMA)

        if window.source is WeatherSource.ARCHIVE:
            url = self._settings.archive_url
            params: dict[str, Any] = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": window.start.isoformat(),
                "end_date": window.end.isoformat(),
            }
        else:
            url = self._settings.forecast_url
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "past_days": self._settings.forecast_past_days,
                "forecast_days": self._settings.forecast_days,
            }

        params["daily"] = ",".join(DAILY_VARIABLES)
        params["timezone"] = "UTC"

        response = self._get(url, params)
        return response.to_frame(farm_code, window.source)

    # -- transport -----------------------------------------------------------
    def _get(self, url: str, params: dict[str, Any]) -> OpenMeteoResponse:
        cache_key = (url, *sorted(params.items()))
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug("weather_cache_hit", url=url)
            return cached

        payload = self._request_with_retry(url, params)
        parsed = OpenMeteoResponse.model_validate(payload)
        self._cache[cache_key] = parsed
        return parsed

    def _request_with_retry(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """Issue the request, retrying throttling and transient faults.

        Built here rather than as a decorator so the retry policy reads from
        settings instead of being fixed at import time.
        """

        @retry(
            retry=retry_if_exception_type(
                (RetryableOpenMeteoError, requests.exceptions.ConnectionError, TimeoutError)
            ),
            stop=stop_after_attempt(self._settings.max_retries + 1),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            reraise=True,
        )
        def attempt() -> dict[str, Any]:
            self._limiter.wait()
            try:
                response = self._session.get(url, params=params, timeout=self._settings.timeout_s)
            except requests.exceptions.Timeout as exc:
                raise RetryableOpenMeteoError(str(exc)) from exc

            self._raise_for_status(response)
            body: dict[str, Any] = response.json()

            # The API reports failures in the body with a 200 in some cases.
            if body.get("error"):
                msg = f"Open-Meteo error: {body.get('reason', 'unknown')}"
                raise OpenMeteoError(msg)
            return body

        return attempt()

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        """Split responses into retryable and terminal.

        A 400 means the request is wrong and will stay wrong; retrying it just
        burns quota. A 429 or 5xx is worth another attempt.
        """
        if response.status_code == 429 or response.status_code >= 500:  # noqa: PLR2004
            msg = f"Open-Meteo returned {response.status_code}; retrying"
            logger.warning("weather_retryable_status", status=response.status_code)
            raise RetryableOpenMeteoError(msg)

        if response.status_code >= 400:  # noqa: PLR2004
            detail = response.text[:200]
            msg = f"Open-Meteo returned {response.status_code}: {detail}"
            raise OpenMeteoError(msg)
