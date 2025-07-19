# djk/pipes/join.py

from typing import Optional
from djk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource

class JoinPipe(Pipe):
    arity = 2

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        arg_string = ptok.get_arg(0)

        allowed_modes = {"left", "inner", "outer"}
        if arg_string not in allowed_modes:
            raise UsageError(
                "join:<mode> must be one of 'left', 'inner', or 'outer'",
                details={"received": arg_string}
            )
        
        self.mode = arg_string
        self.index = 0
        self.pending_right = None
        self.check_right = False
        self.right = None

    def _load_right(self):
        right = self.inputs[1]
        if not isinstance(right, KeyedSource):
            raise UsageError("right input to join must be a KeyedSource")
        self.right = right

    def next(self) -> Optional[dict]:
        if self.right is None:
            self._load_right()

        left = self.inputs[0]

        while True:
            if self.check_right:
                if len(self.pending_right) == 0:
                    return None
                return self.pending_right.pop(0)

            left_rec = left.next()
            if left_rec is None:
                if self.mode != 'outer':
                    return None
                self.pending_right = self.right.get_unlookedup_records()
                self.check_right = True
                continue

            match = self.right.lookup(left_rec)

            if match is not None:
                merged = dict(left_rec)
                merged.update(match)  # right overrides left
                return merged

            if self.mode == "left":
                return left_rec 

            if self.mode == "inner":
                continue  # skip unmatched

