# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Mike Schultz

"""Dictionary-only dotted paths; literal keys take precedence at each level.

Lists are not traversed or flattened. Reads and deletes treat missing or
non-dictionary parents as missing. Writes create missing dictionaries, but
refuse to replace existing scalar, null, or list parents.
"""


def _dot_path_parent(record: dict, path: str, *, create: bool = False):
    """Return the parent, leaf key, and resolved key sequence for a path."""
    current = record
    remaining = path
    keys = []
    while isinstance(current, dict):
        if remaining in current or '.' not in remaining:
            return current, remaining, (*keys, remaining)
        head, remaining = remaining.split('.', 1)
        if head not in current:
            if not create:
                return None
            current[head] = {}
        keys.append(head)
        current = current[head]
    if create:
        raise TypeError(f"Cannot set dot path {path!r}: parent is not a dictionary")
    return None


def dot_path_get(record: dict, path: str, default=None):
    """Read a literal key or dotted path, returning default when missing."""
    parent = _dot_path_parent(record, path)
    if parent is None:
        return default
    container, key, _ = parent
    return container.get(key, default)


def dot_path_delete(record: dict, path: str) -> None:
    """Delete a literal key or dotted path in place; missing paths are a no-op.

    Empty parent dictionaries are retained.
    """
    parent = _dot_path_parent(record, path)
    if parent is not None:
        container, key, _ = parent
        container.pop(key, None)


def dot_path_set(record: dict, path: str, value) -> None:
    """Set a literal key or dotted path in place, creating missing parents.

    Raise TypeError if an existing parent is not a dictionary.
    """
    container, key, _ = _dot_path_parent(record, path, create=True)
    container[key] = value


def dot_path_select(record: dict, paths) -> dict:
    """Build a projection preserving nested structure and literal dotted keys.

    Missing paths are omitted; selected nulls and whole subrecords are kept.
    The input is not mutated. Whole selected values are shared with the input.
    """
    result = {}
    retained = set()
    for path in paths:
        parent = _dot_path_parent(record, path)
        if parent is None:
            continue
        container, key, keys = parent
        if key not in container:
            continue
        if any(keys[:i] in retained for i in range(1, len(keys) + 1)):
            continue
        target = result
        for segment in keys[:-1]:
            target = target.setdefault(segment, {})
        target[keys[-1]] = container[key]
        retained.add(keys)
    return result
