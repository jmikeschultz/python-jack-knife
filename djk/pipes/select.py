# djk/select_pipe.py

from typing import Optional
from djk.base import Pipe, Source, PipeSyntaxError

class SelectFields(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)

        if not arg_string:
            raise PipeSyntaxError("select:<f1,f2,...> requires at least one field")

        self.fields = [f.strip() for f in arg_string.split(',') if f.strip()]
        if not self.fields:
            raise PipeSyntaxError("select must include at least one valid field name")

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None

        return {k: record[k] for k in self.fields if k in record}
