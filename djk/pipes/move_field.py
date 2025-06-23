# djk/pipes/move_field.py
from typing import Optional
from djk.base import Pipe, Source, PipeSyntaxError

class MoveField(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)

        parts = arg_string.split(':')
        if len(parts) != 2:
            raise PipeSyntaxError(
                "mv requires exactly two fields separated by a colon",
                details={"expected": "src_field:dst_field", "received": arg_string}
            )

        self.src, self.dst = parts
        self.count = 0

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        self.count += 1

        if self.src in record:
            record[self.dst] = record.pop(self.src)
            
        return record

