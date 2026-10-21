# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

"""Numeric or frequency summaries for one top-level record field."""

import math
from decimal import Decimal

from pjk.components import Pipe
from pjk.usage import ParsedToken, Usage


PERCENTILES = (10, 25, 50, 75, 90, 95, 99)


def _decimal_value(value: int | float | Decimal) -> Decimal:
    """Use one arithmetic type so ints, floats, and Decimals can be mixed."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    return Decimal.from_float(value)


def _output_number(value: Decimal) -> int | float:
    """Return a value supported by all existing serialization sinks."""
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def is_finite_number(value) -> bool:
    """True only for the explicitly supported, non-boolean numeric types."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, Decimal):
        return value.is_finite()
    return math.isfinite(value)


class ExactQuantileCollector:
    """Retain values and calculate exact, linearly interpolated percentiles.

    For percentile ``p``, the zero-based position is ``(n - 1) * p / 100``.
    A non-integral position is interpolated between its two adjacent values.
    This isolated collector can later be replaced by a bounded-memory sketch.
    """

    def __init__(self):
        self._values: list[Decimal] = []

    def add(self, value: Decimal) -> None:
        self._values.append(value)

    def reset(self) -> None:
        self._values.clear()

    def percentiles(self, requested=PERCENTILES) -> dict[int, Decimal | None]:
        if not self._values:
            return {percentile: None for percentile in requested}

        values = sorted(self._values)
        last = len(values) - 1
        result = {}
        for percentile in requested:
            position = Decimal(last) * Decimal(percentile) / Decimal(100)
            lower_index = int(position)
            fraction = position - lower_index
            lower = values[lower_index]
            if fraction == 0:
                result[percentile] = lower
            else:
                upper = values[lower_index + 1]
                result[percentile] = lower + (upper - lower) * fraction
        return result


class WelfordAccumulator:
    """Online population moments plus the basic numeric extrema and sum."""

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.count = 0
        self.minimum: Decimal | None = None
        self.maximum: Decimal | None = None
        self.total = Decimal(0)
        self.mean = Decimal(0)
        self.m2 = Decimal(0)

    def add(self, value: Decimal) -> None:
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> Decimal | None:
        if not self.count:
            return None
        return self.m2 / self.count


class StatsPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name="stats",
            desc=(
                "reduce the stream to numeric or frequency statistics for one top-level field. "
                "Frequency mode accepts strings, booleans, and finite numbers; absent/null "
                "values are missing and other values are invalid. Frequencies are ordered "
                "by descending count, with ties in first-seen order. Exact frequencies "
                "retain one entry per distinct value in memory."
            ),
            component_class=cls,
        )
        usage.def_arg(name="field", usage="top-level field to summarize")
        usage.def_param(
            name="mode",
            usage="numeric summary or categorical frequencies (numbers, strings, and booleans stay distinct)",
            valid_values={"numeric", "frequency"},
            default="numeric",
        )
        usage.def_syntax("stats:<field>[@mode=numeric|frequency]")
        usage.def_example(
            expr_tokens=["[{bid:1},{bid:2},{bid:3}]", "stats:bid"],
            expect=(
                "{field:'bid', rows:3, valid:3, missing:0, invalid:0, "
                "min:1.0, max:3.0, sum:6.0, mean:2.0, variance:0.6666666666666666, "
                "stddev:0.816496580927726, p10:1.2, p25:1.5, p50:2.0, "
                "p75:2.5, p90:2.8, p95:2.9, p99:2.98}"
            ),
        )
        usage.def_example(
            expr_tokens=["[{color:'red'},{color:'blue'},{color:'red'}]", "stats:color@mode=frequency"],
            expect=(
                "{field:'color', rows:3, valid:3, missing:0, invalid:0, distinct:2, "
                "frequencies:[{value:'red',count:2},{value:'blue',count:1}]}"
            ),
        )
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.field = usage.get_arg("field")
        self.mode = usage.get_param("mode")
        self.reset()

    def reset(self) -> None:
        self.rows = 0
        self.missing = 0
        self.invalid = 0
        self.accumulator = WelfordAccumulator()
        self.quantiles = ExactQuantileCollector()
        self.frequencies = {}
        self.frequency_valid = 0
        self.done = False

    def _summary(self) -> dict:
        valid = self.frequency_valid if self.mode == "frequency" else self.accumulator.count
        summary = {
            "field": self.field,
            "rows": self.rows,
            "valid": valid,
            "missing": self.missing,
            "invalid": self.invalid,
        }

        if self.mode == "frequency":
            summary["distinct"] = len(self.frequencies)
            summary["frequencies"] = sorted(
                self.frequencies.values(), key=lambda item: -item["count"]
            )
            return summary

        if not valid:
            summary.update(
                {name: None for name in ("min", "max", "sum", "mean", "variance", "stddev")}
            )
            summary.update({f"p{percentile}": None for percentile in PERCENTILES})
            return summary

        variance = self.accumulator.variance
        summary.update(
            {
                "min": _output_number(self.accumulator.minimum),
                "max": _output_number(self.accumulator.maximum),
                "sum": _output_number(self.accumulator.total),
                "mean": _output_number(self.accumulator.mean),
                "variance": _output_number(variance),
                "stddev": math.sqrt(_output_number(variance)),
            }
        )
        summary.update(
            {
                f"p{percentile}": _output_number(value)
                for percentile, value in self.quantiles.percentiles().items()
            }
        )
        return summary

    def _add_frequency(self, value) -> None:
        # bool compares equal to 0/1 in Python; tag categories to keep it separate.
        if isinstance(value, bool):
            key = ("boolean", value)
        elif isinstance(value, str):
            key = ("string", value)
        elif is_finite_number(value):
            key = ("number", _decimal_value(value))
            if isinstance(value, Decimal):
                value = _output_number(value)
                if isinstance(value, float) and not math.isfinite(value):
                    self.invalid += 1
                    return
        else:
            self.invalid += 1
            return

        self.frequency_valid += 1
        if key not in self.frequencies:
            self.frequencies[key] = {"value": value, "count": 0}
        self.frequencies[key]["count"] += 1

    def __iter__(self):
        if self.done:
            return

        for record in self.left:
            self.rows += 1
            if not isinstance(record, dict) or self.field not in record or record[self.field] is None:
                self.missing += 1
                continue

            raw_value = record[self.field]
            if self.mode == "frequency":
                self._add_frequency(raw_value)
                continue
            if not is_finite_number(raw_value):
                self.invalid += 1
                continue

            value = _decimal_value(raw_value)
            self.accumulator.add(value)
            self.quantiles.add(value)

        self.done = True
        yield self._summary()
