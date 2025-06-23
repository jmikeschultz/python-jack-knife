# djk/pipes/remove_field.py

from typing import Optional
from djk.base import Pipe, PipeSyntaxError

class RemoveField(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)

        self.fields = [f.strip() for f in arg_string.split(',') if f.strip()]
        if not self.fields:
            raise PipeSyntaxError("rm must include at least one valid field name")
        self.count = 0

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if not record:
            return None

        self.count += 1
        for field in self.fields:
            record.pop(field, None)
            
        return record
