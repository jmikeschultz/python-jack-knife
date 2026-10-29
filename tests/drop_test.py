from pjk.main import execute_tokens


def test_drop_nested_and_top_level_fields():
    execute_tokens([
        "{id:1, old_data:{bid:1, keyword_bids:[{multiplier:1.1}]}, new_data:{bid:2}}",
        'drop:old_data.keyword_bids,new_data,absent.path',
        'expect:{id:1, old_data:{bid:1}}',
    ])


def test_drop_literal_dotted_key_preserves_nested_field():
    execute_tokens([
        '{"old_data.keyword_bids":1, "old_data":{"keyword_bids":[2]}}',
        'drop:old_data.keyword_bids',
        'expect:{old_data:{keyword_bids:[2]}}',
    ])


def test_drop_does_not_traverse_arrays_or_null():
    execute_tokens([
        '[{old_data:null}, {old_data:[{keyword_bids:[1]}]}, {id:1}]',
        'drop:old_data.keyword_bids',
        'expect:[{old_data:null}, {old_data:[{keyword_bids:[1]}]}, {id:1}]',
    ])
