# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import shlex
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Union

from pjk.log import init_stream
from pjk.parser import ExpressionParser, expand_macros
from pjk.registry import ComponentRegistry
from pjk.runtime import enter_pjk_stream, exit_pjk_stream
from pjk.sources.dict_list_source import DictListSource


class PjkStream:
    """
    Lazy record stream from a pjk pipeline.

    Build with a named factory, then iterate:

        for rec in PjkStream.expression('{foo: 1} select:foo -'):
            ...

    The trailing sink token is required for parsing; records are taken from the
    pipe chain (sink.drain is not called).

    With inrecs supplied, the pipeline source is replaced by those records.
    The expression may include a source (which is overridden) or be pipes-only.

    PjkStream never writes CLI history and uses stderr logging (CloudWatch on AWS).
    """

    def __init__(self, tokens: Sequence[str], inrecs: Optional[List[dict]] = None):
        self._tokens = list(tokens)
        self._inrecs = inrecs

    @classmethod
    def expression(cls, expr: str, *, inrecs: Optional[List[dict]] = None) -> "PjkStream":
        """From a CLI-style expression string (shlex-split)."""
        try:
            tokens = shlex.split(expr, comments=True, posix=True)
        except ValueError as e:
            raise ValueError(f"Invalid pjk expression: {e}") from e
        return cls(tokens, inrecs)

    @classmethod
    def tokens(cls, tokens: Sequence[str], *, inrecs: Optional[List[dict]] = None) -> "PjkStream":
        """From an explicit token list."""
        return cls(list(tokens), inrecs)

    @classmethod
    def file(cls, path: Union[Path, str], *, inrecs: Optional[List[dict]] = None) -> "PjkStream":
        """From a .pjk file path (also supports m: macros as a single token via tokens())."""
        path = Path(path)
        if path.suffix != ".pjk":
            raise ValueError(f"PjkStream.file() expects a .pjk path, got: {path}")
        return cls([str(path)], inrecs)

    def __iter__(self) -> Iterator[dict]:
        stream_token = enter_pjk_stream()
        init_stream()
        try:
            yield from self._iter_records()
        finally:
            exit_pjk_stream(stream_token)

    def _iter_records(self) -> Iterator[dict]:
        registry = ComponentRegistry()
        parser = ExpressionParser(registry)
        expanded = expand_macros(self._tokens)

        if self._inrecs is not None:
            source_override = DictListSource(self._inrecs)
            try:
                first_is_source = registry.create_source(expanded[0]) is not None
            except Exception:
                first_is_source = False
            if first_is_source:
                expanded = ["{to_override: 'true'}"] + expanded[1:]
            else:
                expanded = ["{to_override: 'true'}"] + expanded
        else:
            source_override = None

        sink = parser.parse(expanded, source_override=source_override)

        inputs = [sink.input]
        sink.input._get_sources(inputs)
        try:
            for record in sink.input:
                yield record
        finally:
            for inp in inputs:
                inp.close()
