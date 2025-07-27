pjk '{hello: 1}' 'let:there=f.hello + 1' \
    expect:'{"hello": 1, "there": 2}'

pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" [ let:foo:bar over:cars \
    expect:'{"id": 1, "cars": [{"color": "green", "foo": "bar"}, {"color": "blue", "foo": "bar"}]}'

pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" explode:cars \
    expect:'[{"id": 1, "color": "green"}, {"id": 1, "color": "blue"}]'

pjk '[{hello: 1, id: 1}, {there:2, id:2}]' '{id:1}' map:o:id filter:+ \
    expect:'{"hello": 1, "id": 1}'

pjk '[{hello: 1, id: 2}, {there:2, id:2}]' '[{id:1,up:10}, {id:2,up:5}]' map:o:id join:left \
    expect:'[{"id": 2, "up": 5, "hello": 1},{"id": 2, "up": 5, "there": 2}]'

pjk "[{hello: 'there'}, {hello: 'town'}]" 'where:f.hello in "there"' \
    expect:"{hello: 'there'}"

pjk "[{value: 23}, {value:3}]" 'where:f.value > 10' \
    expect:'{value:23}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' map:g:size \
    expect:'{"size": 1, "child": [{"hello": 1}, {"there": 2}]}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' head:1 \
    expect:'{hello: 1, size: 1}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' tail:1 \
    expect:'{there: 2, size: 1}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' as:size:id \
    expect:'[{hello: 1, id: 1}, {there:2, id:1}]'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' rm:size \
    expect:'[{hello: 1}, {there:2}]'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' sel:size,hello \
    expect:'[{hello: 1, size: 1}, {size:1}]'

pjk '[{id: 4}, {id:2}, {id:8}]' sort:-id \
    expect:'[{id: 8}, {id:4}, {id:2}]'

