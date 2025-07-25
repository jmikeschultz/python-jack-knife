# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/tail.py

from typing import Optional
from djk.base import Pipe, ParsedToken, Usage, UsageError

class TailPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='tail',
            desc='take last records of input (when single-threaded)'
        )
        usage.def_arg(name='limit', usage='number of records', is_num=True)
        return usage
    
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.limit = usage.get_arg('limit')

        self.buffer = []
        self.index = 0
        self.ready = False

    def next(self) -> Optional[dict]:
        if not self.ready:
            self._exhaust_all()

        if self.index >= len(self.buffer):
            return None

        record = self.buffer[self.index]
        self.index += 1
        return record

    def _exhaust_all(self):
        self.buffer.clear()
        self.index = 0

        while True:
            record = self.inputs[0].next()
            if record is None:
                break
            self.buffer.append(record)
            if len(self.buffer) > self.limit:
                self.buffer.pop(0)

        self.ready = True

    def reset(self):
        self.index = 0
        self.ready = False
