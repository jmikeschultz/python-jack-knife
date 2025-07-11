# func.py

from typing import Optional
from djk.base import Pipe

class MyFuncPipe(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)
        self.field = arg_string
        self.count = 0

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        self.count += 1
        record[f"marked_{self.field}"] = True
        return record
