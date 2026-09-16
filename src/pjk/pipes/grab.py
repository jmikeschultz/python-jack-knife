# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Any, Iterator, List

from pjk.components import DeepCopyPipe
from pjk.progress import papi
from pjk.usage import ParsedToken, Usage


def _grab_walk(value: Any, segments: List[str]) -> Iterator[dict]:
    """
    Walk a dotted path and yield leaf records.

    - dict at a path segment → descend into that object
    - list at a path segment → continue once per element (flatten)
    - missing / null / empty → yield nothing for that branch
    - leaf dict → yield as-is (no parent/sibling fields merged in)
    - leaf scalar → wrap as {last_segment: value}

    Unlike explode, grab never copies ancestor fields into the output.
    Multiple input records concatenate their leaf yields into one stream.
    """
    if not segments:
        if isinstance(value, dict):
            yield value
        return

    head, rest = segments[0], segments[1:]

    if isinstance(value, list):
        for item in value:
            yield from _grab_walk(item, segments)
        return

    if not isinstance(value, dict):
        return

    if head not in value:
        return

    child = value[head]

    if not rest:
        if isinstance(child, list):
            for item in child:
                if isinstance(item, dict):
                    yield item
                else:
                    yield {head: item}
        elif isinstance(child, dict):
            yield child
        elif child is None:
            return
        else:
            yield {head: child}
        return

    if isinstance(child, list):
        for item in child:
            yield from _grab_walk(item, rest)
    elif isinstance(child, dict):
        yield from _grab_walk(child, rest)
    # mid-path scalar / None: yield nothing


class GrabPipe(DeepCopyPipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='grab',
            desc=(
                "Extract nested records along a dotted path (focus / dive).\n"
                "Walks dicts and flattens lists along the path.\n"
                "Unlike explode, does not copy parent/sibling fields into output.\n"
                "Example: grab:pipeline.tasks  instead of  "
                "select:pipeline explode:pipeline select:tasks explode:tasks"
            ),
            component_class=cls,
        )
        usage.def_arg(
            name='path',
            usage='Dotted field path, e.g. cars or pipeline.tasks',
        )
        usage.def_example(
            expr_tokens=[
                "{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}",
                'grab:cars',
            ],
            expect="[{make: 'ford', size:9}, {make:'bmw', size:4}]",
        )
        usage.def_example(
            expr_tokens=[
                "{ferry:'orca', fleet:{cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}}",
                'grab:fleet.cars',
            ],
            expect="[{make: 'ford', size:9}, {make:'bmw', size:4}]",
        )
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        path = usage.get_arg('path')
        self.segments = [p for p in path.split('.') if p]
        self.recs_in = papi.get_counter(self, 'recs_in', display=False)
        self.recs_out = papi.get_percentage_counter(self, 'recs_out', self.recs_in)

    def reset(self):
        pass

    def __iter__(self):
        for record in self.left:
            self.recs_in.increment()
            for out in _grab_walk(record, self.segments):
                self.recs_out.increment()
                yield out
