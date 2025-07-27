# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sinks/devnull.py

from djk.base import Sink, Source, ParsedToken, Usage

class DevNullSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='devnull',
            desc='Consume all input records and discard them (debug/testing)'
        )
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.count = 0

    def process(self):
        for record in self.input:
            self.count += 1
        print(self.count)

    def display_info(self):
        return {"count": self.count}

    def deep_copy(self):
        return None  # until we implement cross-thread coordination
