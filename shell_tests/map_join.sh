#!/bin/bash

# All of these are expected to succeed with correct output

pjk '[{hello: 1, id: 1}, {there:2, id:2}]' '{id:1}' mapby:id filter:+ \
    expect:'{"hello": 1, "id": 1}'

pjk '[{hello: 1, id: 2}, {there:2, id:2}]' '[{id:1,up:10}, {id:2,up:5}]' mapby:id join:left \
    expect:'[{"id": 2, "up": 5, "hello": 1},{"id": 2, "up": 5, "there": 2}]'

pjk '[{hello: 1, size: 1}, {there:2, size:1}]' groupby:size \
    expect:'{"size": 1, "child": [{"hello": 1}, {"there": 2}]}'

pjk '[{hello: 1, id: 2}, {there:2, id:2}]' \
    '[{id:2,FOO:5},{id:2,GOO:1},{id:4,HOO:4}]' \
    mapby:id join:outer \
    expect:'[{"hello": 1, "id": 2, "GOO": 1},{"there": 2, "id": 2, "GOO": 1},{"id":4, "HOO": 4}]'

pjk '[{id:3}]' '{id:1}' mapby:id join:left \
    expect:'{"id":3}'

pjk '[{id:3}]' '{id:1}' mapby:id join:inner \
    expect:'[]'

pjk '[]' '{id:1,foo:42}' mapby:id join:outer \
    expect:'{"id":1,"foo":42}'

pjk '[{id:1, value:"L"}]' '{id:1, value:"R"}' mapby:id join:left \
    expect:'{"id":1,"value":"R"}'

pjk '[{id:2}]' '{id:1}' mapby:id filter:+ \
    expect:'[]'

pjk '[{id:1},{id:2}]' '{id:1}' mapby:id filter:- \
    expect:'{"id":2}'
