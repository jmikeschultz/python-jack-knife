# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Iterator, List, Optional

from pjk.parser import ExpressionParser, expand_macros
from pjk.registry import ComponentRegistry
from pjk.sources.dict_list_source import DictListSource


class PjkEngine:
    """
    Run a pjk pipeline from a .pjk file, optionally with supplied input records.

    - inrecs supplied: the source in the .pjk file is replaced with inrecs.
      Expression may be full (source + pipes + sink) or pipes-only.
    - inrecs=None: expression.pjk is fully self-contained (source, pipes, sink)
    """

    def __init__(self, inrecs: Optional[List[dict]] = None, pjk_file: str = ""):
        self.inrecs = inrecs
        self.pjk_file = pjk_file

    def __iter__(self) -> Iterator[dict]:
        registry = ComponentRegistry()
        parser = ExpressionParser(registry)
        expanded = expand_macros([self.pjk_file])

        if self.inrecs is not None:
            source_override = DictListSource(self.inrecs)
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
