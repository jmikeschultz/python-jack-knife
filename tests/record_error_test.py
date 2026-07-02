# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pytest

from pjk.common import SafeNamespace
from pjk.errors import RecordProcessingError
from pjk.pipes.let_reduce import do_eval, eval_regular


def test_do_eval_preserves_cause():
    record = {"BILLING_PERIOD_BUDGET": "897.7381"}
    token = "let:daily_budget=int(f.BILLING_PERIOD_BUDGET)/30.0"
    env = {"f": SafeNamespace(record)}

    with pytest.raises(RecordProcessingError) as exc_info:
        do_eval("int(f.BILLING_PERIOD_BUDGET)/30.0", env, record=record, token=token)

    err = exc_info.value
    assert err.token == token
    assert err.record == record
    assert "invalid literal for int()" in str(err.cause)


def test_record_processing_error_str_includes_record_yaml():
    cause = ValueError("invalid literal for int() with base 10: '897.7381'")
    record = {"AD_ID": 1052466454, "BILLING_PERIOD_BUDGET": "897.7381"}
    token = "let:daily_budget=int(f.BILLING_PERIOD_BUDGET)/30.0"
    tokens = ["data.csv", token, "sort:+daily_budget", "-"]

    err = RecordProcessingError(
        token=token,
        record=record,
        cause=cause,
        tokens=tokens,
        token_no=1,
    )
    text = str(err)

    assert "pjk data.csv" in text
    assert "^" in text
    assert "You've got a problem here." in text
    assert "invalid literal for int()" in text
    assert "On this record:" in text
    assert "BILLING_PERIOD_BUDGET: '897.7381'" in text
    assert "Traceback" not in text


def test_eval_regular_raises_on_bad_int():
    record = {"BILLING_PERIOD_BUDGET": "897.7381"}
    token = "let:daily_budget=int(f.BILLING_PERIOD_BUDGET)/30.0"

    with pytest.raises(RecordProcessingError) as exc_info:
        eval_regular("int(f.BILLING_PERIOD_BUDGET)/30.0", record, token=token)

    assert "invalid literal for int()" in str(exc_info.value.cause)


def test_execute_tokens_record_error_no_traceback(monkeypatch, capsys):
    from pjk.main import execute_tokens

    monkeypatch.setenv("PJK_NO_HISTORY", "1")

    tokens = [
        '[{BILLING_PERIOD_BUDGET: "897.7381"}]',
        "let:daily_budget=int(f.BILLING_PERIOD_BUDGET)/30.0",
        "-",
    ]

    with pytest.raises(SystemExit) as exc_info:
        execute_tokens(tokens)

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "You've got a problem here." in err
    assert "invalid literal for int()" in err
    assert "On this record:" in err
    assert "BILLING_PERIOD_BUDGET: '897.7381'" in err
    assert "^" in err
