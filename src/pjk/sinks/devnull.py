# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sinks/devnull.py

from pjk.base import Sink, Source, ParsedToken, Usage

class DevNullSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='devnull',
            desc='Consume all input records and discard them (debug/testing)',
            component_class=cls
        )
        usage.def_example(expr_tokens=['{id:1}', 'devnull'], expect=None)
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

    def process(self):
        for record in self.input:
            pass

    def deep_copy(self):
        # Ask the upstream source to duplicate itself
        source_clone = self.input.deep_copy()
        if source_clone is None:
            return None

        # Create a new DirSink with the next file index
        clone = DevNullSink(self.ptok, self.usage)

        # Wire up the cloned source to the new sink
        clone.add_source(source_clone)

        return clone