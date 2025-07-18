
# these should be moved into proper py tests

echo "error messaging (without blowing up!)"
pjk '{id:1}'

pjk '{id:1}' '{id:2}'

pjk '{id:1}' '{id:2}' -

pjk '{id:1}' '{id:2}' head:1

pjk '[{id:1},{id:2}]' head: -

pjk -

