
# these should be moved into proper py tests

echo 'add_field test'
pjk '{hello: 1}' 'let:there=f.hello + 1' -

echo 'subexp test'
pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" [ let:foo:bar over:cars -

echo 'denorm test'
pjk "{id:1, cars:[{color: 'green'}, {color: 'blue'}]}" denorm:cars -

echo 'map, filter test'
pjk '[{hello: 1, id: 1}, {there:2, id:2}]' '{id:1}' map:id filter:+ -

echo 'map, join test'
pjk "{color: 'green'}" '[{hello: 1, id: 1}, {there:2, id:2}]' map:id join:left -

echo "grep tests"
pjk "{hello: 'there'}" 'grep:f.hello == "there"' -
pjk "{value: 23}" 'grep:f.value > 10' -

echo "group test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' group:size -

echo "head test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' head:1 -

echo "tail test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' tail:1 -

echo "mv test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' mv:size:id -

echo "remove test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' rm:size -

echo "select test"
pjk '[{hello: 1, size: 1}, {there:2, size:1}]' sel:size,hello -

echo "sort test"
pjk '[{hello: 1, id: 1}, {there:2, id:2}]' sort:-id -
