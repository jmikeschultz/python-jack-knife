# djk/pipes/move_field.py
from typing import Optional
from djk.base import Pipe, Source, PipeSyntaxError

class SubExpressionOver(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)
        self.field = arg_string

    def get_over_field(self):
        return self.field

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        return record
        

