pjk '{hello: 1}' 'let:there=f.hello + 1' \
    expect:'{"hello": 1, "there": 2}'

pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" [ let:foo:bar over:cars \
    expect:'{"id": 1, "cars": [{"color": "green", "foo": "bar"}, {"color": "blue", "foo": "bar"}]}'

pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" denorm:cars \
    expect:'[{"id": 1, "color": "blue"}, {"id": 1, "color": "green"}]'

pjk '[{hello: 1, id: 1}, {there:2, id:2}]' '{id:1}' map:id filter:+ \
    expect:'{"hello": 1, "id": 1}'

pjk '[{hello: 1, id: 2}, {there:2, id:2}]' '[{id:1,up:10}, {id:2,up:5}]' map:id join:left \
    expect:'[{"id": 2, "up": 5, "hello": 1},{"id": 2, "up": 5, "there": 2}]'

pjk "[{hello: 'there'}, {hello: 'town'}]" 'grep:f.hello in "there"' \
    expect:"{hello: 'there'}"

pjk "[{value: 23}, {value:3}]" 'grep:f.value > 10' \
    expect:'{value:23}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' group:size \
    expect:'{"size": 1, "child": [{"hello": 1}, {"there": 2}]}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' head:1 \
    expect:'{hello: 1, size: 1}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' tail:1 \
    expect:'{there: 2, size: 1}'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' mv:size:id \
    expect:'[{hello: 1, id: 1}, {there:2, id:1}]'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' rm:size \
    expect:'[{hello: 1}, {there:2}]'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' sel:size,hello \
    expect:'[{hello: 1, size: 1}, {size:1}]'

pjk '[{id: 4}, {id:2}, {id:8}]' sort:-id \
    expect:'[{id: 8}, {id:4}, {id:2}]'

