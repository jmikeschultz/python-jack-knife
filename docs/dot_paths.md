# Dictionary dot paths

`src/pjk/dot_path.py` centralizes dictionary path lookup and mutation. Records
remain ordinary dictionaries. Use normal dictionary access for fixed keys;
use these helpers when a component accepts a dotted field path:

```python
from pjk.dot_path import dot_path_get, dot_path_delete, dot_path_set, dot_path_select

value = dot_path_get(record, "old_data.bid", default=None)
dot_path_delete(record, "old_data.keyword_bids")
dot_path_set(record, "old_data.bid", 2)
selected = dot_path_select(record, ["id", "old_data.bid"])
```

## Resolution rules

- At each dictionary level, an exact key matching the remaining path wins.
  Given `{"a.b": 1, "a": {"b": 2}}`, the path `a.b` resolves to `1`.
  This also applies within nested dictionaries.
- Paths traverse dictionaries only. They do not flatten arrays, interpret
  numeric segments as array indexes, or read Python object attributes.
- `dot_path_get(record, path, default=None)` returns the default for a missing
  key or a non-dictionary parent. An existing null value returns `None`, even
  when a different default is supplied. Pass a sentinel as the default when
  missing and null must be distinguished.
- `dot_path_delete(record, path)` mutates the record. Missing paths and
  non-dictionary parents are a no-op. Empty parent dictionaries remain.
- `dot_path_set(record, path, value)` mutates the record and creates missing
  parent dictionaries. It raises `TypeError` instead of replacing an existing
  scalar, null, or array parent with a dictionary.
- `dot_path_select(record, paths)` builds a new dictionary preserving the
  resolved paths' original structure, including literal dotted keys. Sibling
  selections merge; selecting a whole parent keeps all of it regardless of
  path order. Missing paths are omitted without creating empty parents;
  selected nulls remain. The input is not mutated, but whole selected values
  are shared rather than deep-copied.

## Current consumers

`drop` uses `dot_path_delete` for each comma-separated field:

```sh
pjk audit.json drop:old_data.keyword_bids -
```

`select` uses `dot_path_select` to retain fields at arbitrary dictionary depth:

```sh
pjk audit.json select:id,old_data.bid,old_data.status -
pjk audit.json select:foo.boo.goo -
```

The first command keeps `id` and an `old_data` object containing only `bid` and
`status`. `select:old_data` keeps the entire subrecord. A whole array can be
selected, but paths do not traverse its elements. Previously `select` removed
unwanted top-level keys in place; it now builds a new projection dictionary.

Named `reduce` aggregations (`sum`, `ave`, `min`, `max`, `p50`, `p75`, `p90`)
use `dot_path_get` in place of the former `_record_path` walker:

```sh
pjk audit.json reduce:total=sum:f.old_data.bid -
```

Numeric conversion and aggregation behavior are unchanged. Path resolution now
prefers literal dotted keys; previously named aggregations always split dots
into separate segments. It also no longer traverses attributes of Python
objects supplied by custom sources. Neither implementation flattens arrays.

The `f.` in named aggregation syntax is a required prefix, not part of the
dictionary path. Python expressions such as `where:f.old_data.bid > 1` still
use their existing namespace adapters and Python attribute semantics. They
do not acquire the helpers' literal dotted-key precedence.

## Possible future simplification

Adopt these helpers when a concrete component change benefits from shared path
rules, with tests for that component's existing behavior:

- `grab` has its own path walker because it flattens arrays and yields leaf
  records. Dictionary lookup might be shared later, but replacing the walker
  directly with `dot_path_get` would lose those semantics.
- `let`, expression-based `reduce`, `where`, and conditional `if` blocks use
  namespace adapters. Python performs their attribute traversal. Share lookup
  primitives only if doing so simplifies those adapters without changing
  expression behavior; dotted-string helpers are not a direct replacement.
- Components with direct user-supplied field lookups, such as `sort`, could gain
  dotted paths when needed. Grouping and renaming also require
  explicit decisions about output shape and mutation behavior.

Only `drop`, `select`, and named aggregations have been migrated. `dot_path_set` is
available for future consumers; existing assignment behavior is unchanged.
