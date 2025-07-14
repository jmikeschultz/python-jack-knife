import json
from djk.sources.inline_source import InlineSource

def test_source_list_basic():
    src = InlineSource('[{foo: 1}, {foo: 2}]')

    assert src.next() == {"foo": 1}
    assert src.next() == {"foo": 2}
    assert src.next() is None

