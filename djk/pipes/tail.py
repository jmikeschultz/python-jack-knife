# djk/pipes/tail.py

from typing import Optional
from djk.base import Pipe, SyntaxError

class TailPipe(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)
        try:
            self.limit = int(arg_string)
            if self.limit < 0:
                raise ValueError()
        except ValueError:
            raise SyntaxError("tail:N expects a non-negative integer")

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
