"""Tests for the volume rule.

The rule exists for the frame that passes every schema check and is still
wrong — a tenth of its usual size because the source half-failed. It must flag
that, stay quiet on normal variation, and never judge a pipeline that has no
history to judge it by.
"""

from __future__ import annotations

from datetime import date
from typing import ClassVar

import polars as pl
import pytest
from tests.fakes import FakeControlStore

from smart_agri.io.control import RunStats, RunStatus
from smart_agri.pipelines import get_pipeline, pipeline_names
from smart_agri.pipelines.base import BasePipeline, PipelineContext, RunContext
from smart_agri.quality import (
    DEFAULT_VOLUME_RULE,
    Outcome,
    Severity,
    VolumeRule,
    evaluate_volume,
)

LOGICAL_DATE = date(2026, 8, 1)
STEADY = [100, 100, 100, 100, 100]


def _outcome(current: int, history: list[int], rule: VolumeRule = DEFAULT_VOLUME_RULE) -> Outcome:
    return evaluate_volume(rule, current, history, dataset="widget").outcome


class TestEvaluateVolume:
    @pytest.mark.parametrize("current", [50, 100, 200])
    def test_a_count_inside_the_band_passes(self, current: int) -> None:
        assert _outcome(current, STEADY) is Outcome.PASSED

    @pytest.mark.parametrize("current", [49, 201, 0])
    def test_a_count_outside_the_band_warns(self, current: int) -> None:
        assert _outcome(current, STEADY) is Outcome.WARNED

    def test_an_error_rule_fails_instead(self) -> None:
        rule = VolumeRule(severity=Severity.ERROR)
        assert _outcome(10, STEADY, rule) is Outcome.FAILED

    def test_without_enough_history_the_check_is_skipped(self) -> None:
        """A new pipeline has no baseline; judging it would flag its first runs."""
        result = evaluate_volume(DEFAULT_VOLUME_RULE, 1, [100, 100], dataset="widget")

        assert result.outcome is Outcome.SKIPPED
        assert "fewer than 5" in str(result.details["reason"])

    def test_the_baseline_is_the_median_so_one_outlier_does_not_move_it(self) -> None:
        result = evaluate_volume(
            DEFAULT_VOLUME_RULE, 100, [100, 100, 100, 100, 100_000], dataset="widget"
        )

        assert result.outcome is Outcome.PASSED
        assert result.details["median"] == 100
        assert result.observed_value == pytest.approx(1.0)

    def test_only_the_most_recent_runs_count(self) -> None:
        rule = VolumeRule(history=5)
        old_regime = [10] * 20
        assert _outcome(100, [*STEADY, *old_regime], rule) is Outcome.PASSED

    def test_rows_appearing_in_an_always_empty_dataset_are_flagged(self) -> None:
        result = evaluate_volume(DEFAULT_VOLUME_RULE, 3, [0] * 5, dataset="widget")

        assert result.outcome is Outcome.WARNED
        assert result.observed_value is None

    def test_an_always_empty_dataset_staying_empty_passes(self) -> None:
        assert _outcome(0, [0] * 5) is Outcome.PASSED


class Steady(BasePipeline):
    name = "test.steady"
    dataset = "steady"
    rows: ClassVar[int] = 100

    def extract(self, run: RunContext) -> pl.DataFrame:
        return pl.DataFrame({"x": list(range(self.rows))})

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return frame.height


class TestVolumeInThePipeline:
    @pytest.fixture
    def control(self) -> FakeControlStore:
        control = FakeControlStore()
        for _ in range(5):
            run_id = control.start_run("test.steady", LOGICAL_DATE)
            control.finish_run(run_id, status=RunStatus.SUCCESS, stats=RunStats(100, 100, 0))
        return control

    @pytest.fixture
    def context(self, control: FakeControlStore) -> PipelineContext:
        return PipelineContext(control=control)  # type: ignore[arg-type]

    def test_a_collapsed_input_is_flagged_against_recent_runs(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class Collapsed(Steady):
            rows: ClassVar[int] = 10

        Collapsed(context).run(LOGICAL_DATE)

        (check,) = control.checks_named("volume.rows")
        assert check["outcome"] == "warned"
        assert check["observed_value"] == pytest.approx(0.1)

    def test_an_error_rule_stops_the_run_before_anything_is_written(
        self, context: PipelineContext
    ) -> None:
        loaded: list[int] = []

        class Guarded(Steady):
            rows: ClassVar[int] = 10
            volume_rule = VolumeRule(severity=Severity.ERROR)

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                loaded.append(frame.height)
                return frame.height

        from smart_agri.quality import DataQualityError

        with pytest.raises(DataQualityError, match=r"volume\.rows"):
            Guarded(context).run(LOGICAL_DATE)
        assert loaded == []

    def test_failed_runs_are_not_part_of_the_baseline(self) -> None:
        """A run that crashed half-way wrote a meaningless count."""
        control = FakeControlStore()
        for _ in range(5):
            run_id = control.start_run("test.steady", LOGICAL_DATE)
            control.finish_run(run_id, status=RunStatus.FAILED, stats=RunStats(3, 3, 0))

        Steady(PipelineContext(control=control)).run(LOGICAL_DATE)  # type: ignore[arg-type]

        assert control.checks_named("volume.rows")[0]["outcome"] == "skipped"


class TestWhichPipelinesAreJudged:
    def test_incremental_stages_have_no_volume_rule(self) -> None:
        """A quiet source empties a watermark batch; that is not a failure."""
        incremental = [
            get_pipeline(name)
            for name in pipeline_names()
            if name.startswith(("bronze.", "silver."))
            and getattr(getattr(get_pipeline(name), "spec", None), "incremental", False)
        ]
        bronze_batches = [
            get_pipeline(name)
            for name in pipeline_names()
            if type(get_pipeline(name)).__name__ == "BronzeIncrementalPipeline"
        ]

        assert incremental
        assert bronze_batches
        assert all(p.volume_rule is None for p in [*incremental, *bronze_batches])

    def test_full_recomputes_and_loads_are_judged(self) -> None:
        judged = [
            name
            for name in pipeline_names()
            if name.startswith(("gold.", "load.")) and get_pipeline(name).volume_rule is None
        ]
        assert judged == []
