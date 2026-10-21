import pytest

from pjk.components import KeyedSource
from pjk.main import execute_tokens
from pjk.pipes.join import JoinPipe
from pjk.pipes.map import MapByPipe
from pjk.usage import ParsedToken


def make_map(records):
    token = ParsedToken("mapby:token")
    usage = MapByPipe.usage()
    usage.bind(token)
    result = MapByPipe(token, usage)
    result.add_source(records)
    return result


class LookupSpy(KeyedSource):
    def __init__(self):
        self.records = {("one",): {"token": "one", "right": 10}}
        self.lookup_calls = 0
        self.lookup_item_calls = 0

    def lookup(self, left_rec):
        self.lookup_calls += 1
        return self.records.get((left_rec.get("token"),))

    def lookup_item(self, left_rec):
        self.lookup_item_calls += 1
        key = (left_rec.get("token"),)
        record = self.records.get(key)
        return (key, record) if record is not None else None

    def keyed_items(self):
        return self.records.items()


def make_join(mode, left, right):
    token = ParsedToken(f"join:{mode}")
    usage = JoinPipe.usage()
    usage.bind(token)
    result = JoinPipe(token, usage)
    result.left = left
    result.right = right
    return result


@pytest.mark.parametrize("mode", ["left", "inner"])
def test_non_outer_join_does_not_request_match_keys(mode):
    right = LookupSpy()

    list(make_join(mode, [{"token": "one"}], right))

    assert right.lookup_calls == 1
    assert right.lookup_item_calls == 0


def test_only_outer_join_requests_match_keys():
    right = LookupSpy()

    list(make_join("outer", [{"token": "one"}], right))

    assert right.lookup_calls == 0
    assert right.lookup_item_calls == 1


def test_map_lookup_does_not_mutate_map():
    token_map = make_map([
        {"token": "one", "score": 1},
        {"token": "two", "score": 2},
    ])

    assert token_map.lookup({"token": "one"}) == {"token": "one", "score": 1}
    assert token_map.lookup({"token": "one"}) == {"token": "one", "score": 1}
    assert list(token_map.keyed_items()) == [
        (("one",), {"token": "one", "score": 1}),
        (("two",), {"token": "two", "score": 2}),
    ]


def test_outer_join_tracks_matches_without_mutating_map():
    expected = """[
        {token:'one', left:1, right:10},
        {token:'one', left:2, right:10},
        {token:'missing', left:3},
        {token:'two', right:20}
    ]"""

    execute_tokens([
        "[{token:'one', left:1}, {token:'one', left:2}, "
        "{token:'missing', left:3}]",
        "[{token:'one', right:10}, {token:'two', right:20}]",
        "mapby:token",
        "join:outer",
        f"expect:{expected}",
    ])


def test_over_loads_independent_join_map_once(monkeypatch):
    load_count = 0
    original_load = MapByPipe.load

    def counted_load(token_map):
        nonlocal load_count
        if not token_map.is_loaded:
            load_count += 1
        return original_load(token_map)

    monkeypatch.setattr(MapByPipe, "load", counted_load)

    expected = """[
        {id:1, stoks:[{token:'one', score:1}]},
        {id:2, stoks:[{token:'two', score:2}]}
    ]"""
    execute_tokens([
        "[{id:1, stoks:[{token:'one'}]}, {id:2, stoks:[{token:'two'}]}]",
        "[",
        "[{token:'one', score:1}, {token:'two', score:2}]",
        "mapby:token",
        "join:left",
        "over:stoks",
        f"expect:{expected}",
    ])

    assert load_count == 1


def test_over_still_resets_independent_non_lookup_branch():
    expected = """[
        {id:1, items:[{value:'parent-one'}, {value:'static'}]},
        {id:2, items:[{value:'parent-two'}, {value:'static'}]}
    ]"""
    execute_tokens([
        "[{id:1, items:[{value:'parent-one'}]}, "
        "{id:2, items:[{value:'parent-two'}]}]",
        "[",
        "[{value:'static'}, {value:'unused'}]",
        "head:1",
        "join:concat",
        "over:items",
        f"expect:{expected}",
    ])
