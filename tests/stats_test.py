import csv
import json
import math
from decimal import Decimal

import pytest

from pjk import PjkStream
from pjk.main import execute_tokens
from pjk.pipes.factory import PipeFactory


def stats(records):
    return list(PjkStream.expression("stats:value -", inrecs=records))


def test_normal_numeric_input_and_population_statistics():
    result = stats([{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}])

    assert len(result) == 1
    assert result[0]["field"] == "value"
    numeric_result = {key: value for key, value in result[0].items() if key != "field"}
    assert numeric_result == pytest.approx(
        {
            "rows": 4,
            "valid": 4,
            "missing": 0,
            "invalid": 0,
            "min": 1,
            "max": 4,
            "sum": 10,
            "mean": 2.5,
            "variance": 1.25,
            "stddev": math.sqrt(1.25),
            "p10": 1.3,
            "p25": 1.75,
            "p50": 2.5,
            "p75": 3.25,
            "p90": 3.7,
            "p95": 3.85,
            "p99": 3.97,
        }
    )


def test_int_float_and_decimal_are_valid_and_strings_are_not_coerced():
    result = stats(
        [
            {"value": 1},
            {"value": 2.5},
            {"value": Decimal("3.5")},
            {"value": "3.14"},
            {"value": [4]},
        ]
    )[0]

    assert result["valid"] == 3
    assert result["invalid"] == 2
    assert result["sum"] == 7


def test_missing_none_invalid_boolean_nan_and_infinity_counts():
    records = [
        {},
        {"value": None},
        {"value": "no"},
        {"value": object()},
        {"value": True},
        {"value": False},
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": float("-inf")},
        {"value": Decimal("NaN")},
        {"value": Decimal("Infinity")},
    ]

    result = stats(records)[0]
    assert result["rows"] == len(records)
    assert result["valid"] == 0
    assert result["missing"] == 2
    assert result["invalid"] == 9


@pytest.mark.parametrize("records", [[], [{}, {"value": None}, {"value": "1"}]])
def test_no_valid_values_emit_one_record_with_null_numeric_fields(records):
    result = stats(records)

    assert len(result) == 1
    assert result[0]["rows"] == len(records)
    for key in (
        "min",
        "max",
        "sum",
        "mean",
        "variance",
        "stddev",
        "p10",
        "p25",
        "p50",
        "p75",
        "p90",
        "p95",
        "p99",
    ):
        assert result[0][key] is None


def test_exact_percentiles_use_n_minus_one_linear_interpolation():
    result = stats([{"value": 0}, {"value": 10}, {"value": 20}])[0]

    assert {key: result[key] for key in ("p10", "p25", "p50", "p75", "p90", "p95", "p99")} == {
        "p10": 2,
        "p25": 5,
        "p50": 10,
        "p75": 15,
        "p90": 18,
        "p95": 19,
        "p99": 19.8,
    }


def test_pipe_is_registered_and_usage_binds_one_field():
    pipe = PipeFactory().create("stats:bid")

    assert pipe is not None
    assert pipe.field == "bid"
    assert pipe.mode == "numeric"
    assert pipe.usage.get_token_syntax() == "stats:<field>[@mode=numeric|frequency]"


def test_cli_result_pipes_to_json_and_csv_sinks(tmp_path):
    json_path = tmp_path / "stats.json"
    csv_path = tmp_path / "stats.csv"
    source = "[{bid:1},{bid:2},{other:3},{bid:'bad'}]"

    execute_tokens([source, "stats:bid", str(json_path)])
    execute_tokens([source, "stats:bid", str(csv_path)])

    json_records = [json.loads(line) for line in json_path.read_text().splitlines()]
    assert len(json_records) == 1
    assert json_records[0]["rows"] == 4
    assert json_records[0]["valid"] == 2

    with csv_path.open(newline="") as csv_file:
        csv_records = list(csv.DictReader(csv_file))
    assert len(csv_records) == 1
    assert csv_records[0]["field"] == "bid"


def frequencies(records):
    return list(PjkStream.expression("stats:value@mode=frequency -", inrecs=records))[0]


def test_frequency_strings_are_ranked_with_first_seen_ties():
    result = frequencies([
        {"value": value} for value in ["blue", "red", "green", "red", "green", "red", ""]
    ])
    assert result == {
        "field": "value", "rows": 7, "valid": 7, "missing": 0, "invalid": 0,
        "distinct": 4,
        "frequencies": [
            {"value": "red", "count": 3},
            {"value": "green", "count": 2},
            {"value": "blue", "count": 1},
            {"value": "", "count": 1},
        ],
    }


def test_frequency_preserves_string_boolean_and_numeric_categories():
    result = frequencies([
        {"value": value} for value in [True, 1, "1", 1.0, False, 0, "true", Decimal("1")]
    ])
    assert result["valid"] == 8
    assert result["distinct"] == 6
    entries = result["frequencies"]
    assert entries[0] == {"value": 1, "count": 3}
    assert [(type(item["value"]), item["value"], item["count"]) for item in entries[1:]] == [
        (bool, True, 1), (str, "1", 1), (bool, False, 1), (int, 0, 1), (str, "true", 1),
    ]


def test_frequency_counts_missing_and_invalid_values():
    result = frequencies([{}, {"value": None}] + [
        {"value": value} for value in [[], {}, object(), float("nan"), float("inf"), Decimal("NaN")]
    ])
    assert result == {
        "field": "value", "rows": 8, "valid": 0, "missing": 2, "invalid": 6,
        "distinct": 0, "frequencies": [],
    }


def test_empty_frequency_stream_emits_one_summary():
    assert frequencies([]) == {
        "field": "value", "rows": 0, "valid": 0, "missing": 0, "invalid": 0,
        "distinct": 0, "frequencies": [],
    }


def test_frequency_reset_clears_categories():
    pipe = PipeFactory().create("stats:value@mode=frequency")
    pipe.add_source([{"value": "old"}])
    assert list(pipe)[0]["valid"] == 1
    assert list(pipe) == []
    pipe.reset()
    pipe.left = [{"value": "new"}]
    assert list(pipe)[0]["frequencies"] == [{"value": "new", "count": 1}]


def test_unknown_stats_mode_is_rejected():
    from pjk.usage import TokenError

    with pytest.raises(TokenError):
        PipeFactory().create("stats:value@mode=automatic")


def test_frequency_cli_output_preserves_json_value_types(tmp_path):
    output = tmp_path / "frequencies.json"
    execute_tokens([
        "[{value:'1'},{value:1},{value:true},{value:'1'}]",
        "stats:value@mode=frequency", str(output),
    ])
    result = json.loads(output.read_text())
    assert [(type(item["value"]), item["value"], item["count"]) for item in result["frequencies"]] == [
        (str, "1", 2), (int, 1, 1), (bool, True, 1),
    ]
