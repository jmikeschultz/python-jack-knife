from pjk.main import execute_tokens
from pathlib import Path
import pytest

HERE = Path(__file__).parent
INPUT = HERE / "data" / "subexp_data.json" 
NESTED_ANSWER = """{"ferry": [{"name": "orca", "cars": [{"make": "ford", "size": 3, "foo": "bar"}, {"make": "bmw", "size": 44, "foo": "bar"}, {"make": "honda", "size": 4, "foo": "bar"}], "goo": "hoo"}, {"name": "victoria", "cars": [{"make": "toyota", "size": 9, "foo": "bar"}, {"make": "nissan", "size": 10, "foo": "bar"}, {"make": "vw", "size": 22, "foo": "bar"}], "goo": "hoo"}]}"""
ONE_ANSWER = """{"ferry": [{"name": "orca", "cars": [{"make": "ford", "size": 3}, {"make": "bmw", "size": 44}, {"make": "honda", "size": 4}], "foo": "bar"}, {"name": "victoria", "cars": [{"make": "toyota", "size": 9}, {"make": "nissan", "size": 10}, {"make": "vw", "size": 22}], "foo": "bar"}]}"""
def test_thing():
    text = INPUT.read_text(encoding="utf-8")   # or DATA.read_bytes()

def test_nested():
    execute_tokens([str(INPUT),
                    "[",
                    "[",
                    "let:foo:bar",
                    "over:cars",
                    "let:goo:hoo",
                    "over:ferry",
                    f"expect:{NESTED_ANSWER}"])
    
def test_one():
    execute_tokens([str(INPUT),
                    "[",
                    "let:foo:bar",
                    "over:ferry",
                    "sort:-foo",
                    f"expect:{ONE_ANSWER}"])

def test_over_exposes_only_requested_parent_fields_temporarily():
    expected = """{"parent":"P","color":"outer","items":[{"child":"C","seen":"P","parent_color":"outer","copied":"P"}]}"""
    execute_tokens([
        "{parent:'P', color:'outer', items:[{child:'C'}]}",
        "[",
        "let:seen=f.parent",
        "let:parent_color=f.color",
        "as:parent:copied",
        "over:items@parents=parent,color",
        f"expect:{expected}",
    ])

def test_over_without_parents_does_not_expose_parent_fields():
    expected = """{"parent":"P","items":[{"child":"C","seen":null}]}"""
    execute_tokens([
        "{parent:'P', items:[{child:'C'}]}",
        "[",
        "let:seen=f.parent",
        "over:items",
        f"expect:{expected}",
    ])

def test_over_rejects_parent_field_collision(capsys):
    with pytest.raises(SystemExit) as exc_info:
        execute_tokens([
            "{parent:'P', items:[{child:'C', parent:'existing'}]}",
            "[",
            "let:seen=f.parent",
            "over:items@parents=parent",
            "devnull",
        ])

    assert exc_info.value.code == 2
    error = capsys.readouterr().err
    assert "parent-field collision" in error
    assert "parent" in error

def test_over_rejects_missing_requested_parent_field(capsys):
    with pytest.raises(SystemExit) as exc_info:
        execute_tokens([
            "{parent:'P', items:[{child:'C'}]}",
            "[",
            "let:seen=f.missing",
            "over:items@parents=missing",
            "devnull",
        ])

    assert exc_info.value.code == 2
    error = capsys.readouterr().err
    assert "@parents field not found" in error
    assert "missing" in error
    
def test_syntax_error():
    with pytest.raises(SystemExit) as e:
        execute_tokens([str(INPUT),
                    "[",
                    "let:foo:bar",
                    "over:cars",
                    "over:ferry",
                    "devnull"])
        assert e.value == 2

def test_syntax_error2():
    with pytest.raises(SystemExit) as e:
        execute_tokens([str(INPUT),
                    "[",
                    "[",
                    "let:foo:bar",
                    "over:ferry",
                    "sort:-foo",
                    "devnull"])
        assert e.value == 2
