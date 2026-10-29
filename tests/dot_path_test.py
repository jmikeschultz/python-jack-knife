import pytest

from pjk.dot_path import dot_path_delete, dot_path_get, dot_path_set


def test_get_distinguishes_missing_from_null():
    missing = object()
    record = {'old_data': {'bid': 1, 'end_date': None}}
    assert dot_path_get(record, 'old_data.bid') == 1
    assert dot_path_get(record, 'old_data.end_date', missing) is None
    assert dot_path_get(record, 'old_data.absent', missing) is missing


@pytest.mark.parametrize('parent', [None, 1, 'text', [{'bid': 1}], {}])
def test_missing_or_non_dictionary_parent(parent):
    record = {'old_data': parent}
    missing = object()
    assert dot_path_get(record, 'old_data.bid', missing) is missing
    dot_path_delete(record, 'old_data.bid')
    assert record == {'old_data': parent}


@pytest.mark.parametrize('prefix', ['', 'outer.'])
def test_literal_dotted_keys_take_precedence_for_all_operations(prefix):
    inner = {'a.b': 1, 'a': {'b': 2}}
    record = {'outer': inner} if prefix else inner
    path = prefix + 'a.b'
    assert dot_path_get(record, path) == 1
    dot_path_set(record, path, 3)
    assert inner == {'a.b': 3, 'a': {'b': 2}}
    dot_path_delete(record, path)
    assert inner == {'a': {'b': 2}}


def test_delete_preserves_siblings_and_empty_parents():
    record = {'id': 1, 'old_data': {'bid': 2, 'keyword_bids': [3]}}
    dot_path_delete(record, 'old_data.keyword_bids')
    assert record == {'id': 1, 'old_data': {'bid': 2}}
    dot_path_delete(record, 'old_data.bid')
    assert record == {'id': 1, 'old_data': {}}


def test_set_creates_missing_parents_and_replaces_leaf():
    record = {}
    dot_path_set(record, 'old_data.details.bid', 1)
    assert record == {'old_data': {'details': {'bid': 1}}}
    dot_path_set(record, 'old_data.details.bid', None)
    assert dot_path_get(record, 'old_data.details.bid', 'missing') is None


@pytest.mark.parametrize('parent', [None, 1, 'text', []])
def test_set_refuses_to_overwrite_non_dictionary_parents(parent):
    record = {'old_data': parent}
    with pytest.raises(TypeError, match='parent is not a dictionary'):
        dot_path_set(record, 'old_data.bid', 1)
    assert record == {'old_data': parent}
