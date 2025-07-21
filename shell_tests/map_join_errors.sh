#!/bin/bash

# These should all produce friendly UsageError or TokenError, not crash

# filter with missing arg
pjk '{id:1}' filter: -

# filter with bad arg
pjk '{id:1}' filter:1 -

# map with bad 'how'
pjk '{id:1}' map:x:id -

# map with missing key
pjk '{id:1}' map:o -

# map with missing how
pjk '{id:1}' map::id -
