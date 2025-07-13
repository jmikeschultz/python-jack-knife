# djk/pipes/head.py

from typing import Optional
from djk.base import Pipe, ParsedToken, SyntaxError

class HeadPipe(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)
        try:
            self.limit = int(ptok.get_arg(0))
            if self.limit < 0:
                raise ValueError()
        except ValueError:
            raise SyntaxError("head:N expects a non-negative integer")
        self.count = 0

    def next(self) -> Optional[dict]:
        if self.count >= self.limit:
            return None
        record = self.inputs[0].next()
        if record is None:
            return None
        self.count += 1
        return record
    
    def reset(self):
        self.count = 0
