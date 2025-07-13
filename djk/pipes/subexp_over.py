# djk/pipes/move_field.py
from typing import Optional
from djk.base import Pipe, Source, ParsedToken, SyntaxError

class SubExpressionOver(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)
        self.over_arg = ptok.get_arg(0)

    def get_over_arg(self):
        return self.over_arg

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        return record
        

