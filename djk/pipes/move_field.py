# djk/pipes/move_field.py
from typing import Optional
from djk.base import Pipe, Source, ParsedToken, SyntaxError

class MoveField(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)

        self.src = ptok.get_arg(0)
        self.dst = ptok.get_arg(1)

        '''
        if len(parts) != 2:
            raise SyntaxError(
                "mv requires exactly two fields separated by a colon",
                details={"expected": "src_field:dst_field", "received": arg_string}
            )
        '''

        self.count = 0

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        self.count += 1

        if self.src in record:
            record[self.dst] = record.pop(self.src)
            
        return record

