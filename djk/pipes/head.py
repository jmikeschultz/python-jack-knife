# djk/pipes/head.py

from typing import Optional
from djk.base import Pipe, ParsedToken, Usage, UsageError

class HeadPipe(Pipe):
    @classmethod
    def define_usage(cls):
        usage = Usage(
            name='head',
            desc='take first records of source (when single-threadedE)'
        )
        usage.def_arg(name='limit', usage='number of records', is_num=True)
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.limit = usage.get_arg('limit')
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
