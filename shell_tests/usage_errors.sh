
# these should be moved into proper py tests

echo "error messaging (without blowing up!)"
pjk '{id:1}'

pjk '{id:1}' '{id:2}'

pjk '{id:1}' '{id:2}' -

pjk '{id:1}' '{id:2}' head:1

pjk '[{id:1},{id:2}]' head: -

pjk '[{id:1},{id:2}]' head -

pjk -

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y:z

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y@pause

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y@pause=

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y@pause=lsk

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y@nope=lsk

