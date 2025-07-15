import re
from typing import Optional
from types import SimpleNamespace
from djk.base import Pipe, ParsedToken, Usage, UsageError
from djk.pipes.common import SafeNamespace

class GrepPipe(Pipe):
    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)

        arg_string = ptok.whole_token.split(':', 1)[-1]
        self.expr = arg_string.strip()
        try:
            self.code = compile(self.expr, '<grep>', 'eval')
        except Exception as e:
            raise UsageError(f"Invalid grep expression: {self.expr}") from e

    def next(self) -> Optional[dict]:
        while True:
            record = self.inputs[0].next()
            if record is None:
                return None

            f = SafeNamespace(record)
            try:
                if eval(self.code, {}, {'f': f}):
                    return record
            except Exception:
                continue  # skip record on bad field, math error, etc.
