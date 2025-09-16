# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/tail.py

from pjk.base import Pipe, ParsedToken, Usage

class TailPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='tail',
            desc='take last records of input (when single-threaded)',
            component_class=cls
        )
        usage.def_arg(name='limit', usage='number of records', is_num=True)
        usage.def_example(expr_tokens=['[{id:1}, {id:2}]', 'tail:1'], expect="{id:2}")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.limit = usage.get_arg('limit')

        self.buffer = []
        self.ready = False

    def reset(self):
        self.buffer.clear()
        self.ready = False

    def __iter__(self):
        if not self.ready:
            for record in self.left:
                self.buffer.append(record)
                if len(self.buffer) > self.limit:
                    self.buffer.pop(0)
            self.ready = True

        yield from self.buffer
