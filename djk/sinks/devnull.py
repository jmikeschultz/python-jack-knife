# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sinks/devnull.py

from djk.base import Sink, Source, ParsedToken, Usage

class DevNullSink(Sink):
    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.count = 0

    def process(self):
        while True:
            record = self.input.next()
            if record is None:
                break
            self.count += 1
        print(self.count)

    def display_info(self):
        return {"count": self.count}
    
    def deep_copy(self):
        return None # until we can synchronize well across threads
        source_clone = self.input.deep_copy()
        if not source_clone:
            return None
        
        return DevNullSink(source_clone)
