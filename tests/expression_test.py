import pytest

from pjk.errors import RecordProcessingError
from pjk.expression import CompiledExpression
from pjk.usage import UsageError


def test_compiled_expression_reads_nested_fields_and_materializes_results():
    expression = CompiledExpression(
        "{'copy': f.details, 'items': f.items}",
        "<test>",
    )
    record = {
        "details": {"value": 7},
        "items": [{"value": 1}, {"value": 2}],
    }

    result = expression.evaluate(record, token="test:expression")

    assert result == {
        "copy": {"value": 7},
        "items": [{"value": 1}, {"value": 2}],
    }
    assert isinstance(result["copy"], dict)
    assert isinstance(result["items"], list)


def test_compiled_expression_supports_shared_json_and_re_environment():
    expression = CompiledExpression(
        "json.loads(f.payload)['value'] if re.match('^yes', f.answer) else 0",
        "<test>",
    )

    result = expression.evaluate(
        {"payload": '{"value": 4}', "answer": "yes please"},
        token="test:expression",
    )

    assert result == 4


def test_reducing_expression_promotes_scalar_for_root_comprehension():
    expression = CompiledExpression(
        "[value * 2 for value in f.amount]",
        "<test>",
        reducing=None,
    )

    assert expression.evaluate(
        {"amount": 3},
        token="test:expression",
    ) == [6]


def test_compiled_expression_wraps_runtime_error_with_record_and_token():
    expression = CompiledExpression("1 / f.value", "<test>")
    record = {"value": 0}

    with pytest.raises(RecordProcessingError) as exc_info:
        expression.evaluate(record, token="test:1 / f.value")

    assert exc_info.value.token == "test:1 / f.value"
    assert exc_info.value.record == record
    assert isinstance(exc_info.value.cause, ZeroDivisionError)


def test_compiled_expression_uses_caller_syntax_error_prefix():
    with pytest.raises(UsageError, match="Invalid test expression"):
        CompiledExpression(
            "f.value +",
            "<test>",
            syntax_error_prefix="Invalid test expression",
        )
