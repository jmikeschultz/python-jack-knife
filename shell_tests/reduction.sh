#!/bin/bash
set -e

echo "--- list comprehension"
pjk '[{i:1},{i:2}]' 'reduce:xs=[x for x in f.i]' \
    expect:'{xs: [1, 2]}'

#echo "--- set comprehension" have no way to test this with expect tho it should work
#pjk '[{tags:["a","b"]},{tags:["b","c"]}]' 'reduce:uniq={x for x in f.tags}' \
#    expect:'{uniq: ["a", "b", "c"]}'

echo "--- dict comprehension"
pjk '[{kv:[["a",1],["b",2]]},{kv:[["b",3],["c",4]]}]' \
    'reduce:kv={k: v for k, v in f.kv}' \
    expect:'{kv: {a: 1, b: 3, c: 4}}'

echo "--- scalar += list"
pjk '[{i:1},{i:2}]' 'reduce:xs=[x for x in f.i]' \
    expect:'{xs: [1, 2]}'

echo "--- list of lists with +="
pjk '[{i:[1,2]},{i:[3]}]' 'reduce:xs+=[f.i]' \
    expect:'{xs: [[1, 2], [3]]}'

echo "--- flatten list via comp"
pjk '[{i:[1,2]},{i:[3]}]' 'reduce:xs=[x for x in f.i]' \
    expect:'{xs: [1, 2, 3]}'

echo "--- sum via +="
pjk '[{i:3},{i:4}]' 'reduce:sum+=f.i' \
    expect:'{sum: 7}'

echo "--- product via *="
pjk '[{i:2},{i:5}]' 'reduce:prod*=f.i' \
    expect:'{prod: 10}'

echo "--- subtraction via -= (with implied acc)"
pjk '[{i:10},{i:3}]' 'reduce:diff-=f.i' \
    expect:'{diff: -13}'

pjk "{id:1, cars:[{size: 24}, {size: 43}]}" [ reduce:^tot+=f.size over:cars \
    expect:'{"id": 1, "cars": [{"size": 24}, {"size": 43}], "tot":67}'

echo "--- let: preserves scalar access"
pjk '[{i:42}]' 'let:x=f.i' \
    expect:'{i: 42, x: 42}'

echo "--- reduce: lifts scalar for comp"
pjk '[{i:42}]' 'reduce:xs=[x for x in f.i]' \
    expect:'{xs: [42]}'

echo "--- reject scalar RHS without op"
pjk '[{i:3}]' 'reduce:x=f.i' - || echo "ok: expected failure"

echo "--- reject literal dict without op"
pjk '[{i:3}]' 'reduce:x={"a": 1}' - || echo "ok: expected failure"

