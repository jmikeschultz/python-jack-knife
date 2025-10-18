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
                    f"expect:{ONE_ANSWER}"])
    
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