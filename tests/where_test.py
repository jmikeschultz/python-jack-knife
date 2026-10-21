# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Mike Schultz

import pytest

from pjk.main import execute_tokens


@pytest.mark.parametrize(
    ("expression", "error"),
    [
        ("f.foo == null", "name 'null' is not defined"),
        ("f.foo == typo", "name 'typo' is not defined"),
        ("1 / 0", "division by zero"),
    ],
)
def test_where_reports_evaluation_errors(expression, error, capsys):
    token = f"where:{expression}"

    with pytest.raises(SystemExit) as exc_info:
        execute_tokens(["[{foo:null}]", token, "devnull"])

    assert exc_info.value.code == 2
    stderr = capsys.readouterr().err
    assert error in stderr
    assert token in stderr
    assert "On this record:" in stderr
    assert "foo: null" in stderr
    assert "^" in stderr
    assert "Traceback" not in stderr


@pytest.mark.parametrize("expression", ["f.foo == None", "f.foo is None"])
def test_where_none_matches_null_and_missing_fields(expression):
    execute_tokens([
        "[{foo:null}, {foo:1}, {bar:2}]",
        f"where:{expression}",
        "expect:[{foo:null}, {bar:2}]",
    ])


def test_where_accepts_null_as_a_string_and_field_name():
    execute_tokens([
        "[{foo:'null', null:1}, {foo:'other', null:1}]",
        "where:f.foo == 'null' and f.null == 1",
        "expect:[{foo:'null', null:1}]",
    ])
