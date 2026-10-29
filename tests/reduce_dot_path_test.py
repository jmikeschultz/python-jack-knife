import pytest

from pjk.main import execute_tokens


@pytest.mark.parametrize('aggregation,expected', [
    ('sum', 10.5), ('ave', 5.25), ('min', 2), ('max', 8.5),
    ('p50', 2), ('p75', 8.5), ('p90', 8.5),
])
def test_named_aggregations_read_nested_values_and_ignore_missing(aggregation, expected):
    execute_tokens([
        '[{old_data:{bid:2}}, {old_data:{bid:"8.5"}}, '
        '{old_data:{bid:null}}, {old_data:{bid:"invalid"}}, '
        '{}, {old_data:null}, {old_data:1}, {old_data:[{bid:100}]}]',
        f'reduce:result={aggregation}:f.old_data.bid',
        f'expect:{{result:{expected}}}',
    ])


@pytest.mark.parametrize('record,path', [
    ('{"old_data.bid":3, "old_data":{"bid":100}}', 'old_data.bid'),
    ('{"old_data":{"details.bid":3, "details":{"bid":100}}}', 'old_data.details.bid'),
])
def test_named_aggregation_prefers_literal_dotted_keys(record, path):
    execute_tokens([
        record,
        f'reduce:result=sum:f.{path}',
        'expect:{result:3}',
    ])
