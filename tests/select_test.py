from copy import deepcopy

import pytest

from pjk.dot_path import dot_path_select
from pjk.main import execute_tokens


def test_select_arbitrary_depth_merges_siblings():
    execute_tokens([
        '{id:1, foo:{boo:{goo:2, other:3, unwanted:4}, unwanted:5}, unwanted:6}',
        'select:id,foo.boo.goo,foo.boo.other',
        'expect:{id:1, foo:{boo:{goo:2, other:3}}}',
    ])


def test_select_preserves_literal_dotted_keys_at_each_level():
    execute_tokens([
        '{"a.b":1, "a":{"b":2}, "foo":{"boo.goo":3, "boo":{"goo":4}}}',
        'select:a.b,foo.boo.goo',
        'expect:{"a.b":1, "foo":{"boo.goo":3}}',
    ])


def test_select_skips_missing_and_non_dictionary_parents_but_keeps_null_leaves():
    execute_tokens([
        '{foo:{empty:null}, nil:null, scalar:1, items:[{value:2}]}',
        'select:foo.empty,foo.absent.value,nil.value,scalar.value,items.value,absent',
        'expect:{foo:{empty:null}}',
    ])


@pytest.mark.parametrize('paths', [
    ['foo', 'foo.boo.goo'],
    ['foo.boo.goo', 'foo'],
    ['foo.boo', 'foo.boo.goo', 'foo.boo.goo'],
    ['foo.boo.goo', 'foo.boo'],
])
def test_select_whole_parent_wins_regardless_of_order_without_mutating_input(paths):
    record = {'foo': {'boo': {'goo': 1, 'other': 2}}, 'unwanted': 3}
    original = deepcopy(record)
    result = dot_path_select(record, paths)
    assert result == {'foo': {'boo': {'goo': 1, 'other': 2}}}
    assert result is not record
    assert record == original


def test_select_keeps_whole_arrays_and_top_level_fields():
    execute_tokens([
        '{id:1, items:[{value:2}], unwanted:3}',
        'select:id,items',
        'expect:{id:1, items:[{value:2}]}',
    ])


def test_select_missing_paths_does_not_create_empty_parents():
    assert dot_path_select({'foo': {'boo': {}}}, ['foo.boo.absent']) == {}


def test_select_partial_projection_does_not_mutate_input():
    record = {'foo': {'boo': {'goo': 1, 'other': 2}}, 'unwanted': 3}
    original = deepcopy(record)
    result = dot_path_select(record, ['foo.boo.goo'])
    assert result == {'foo': {'boo': {'goo': 1}}}
    assert record == original
    assert result['foo'] is not record['foo']
    assert result['foo']['boo'] is not record['foo']['boo']
