# djk/pipes/join.py

from typing import Optional
from djk.base import Pipe, SyntaxError, KeyedSource

class JoinPipe(Pipe):
    arity = 2

    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)

        allowed_modes = {"left", "inner", "outer"}
        if arg_string not in allowed_modes:
            raise SyntaxError(
                "join:<mode> must be one of 'left', 'inner', or 'outer'",
                details={"received": arg_string}
            )
        self.mode = arg_string
        self.index = 0
        self.pending_left = []
        self.right_lookup = None
        self.key_field = None

    def _load_right(self):
        right = self.inputs[1]
        if not isinstance(right, KeyedSource):
            raise SyntaxError("right input to join must be a KeyedSource")
        self.right_lookup = right
        self.key_field = right.get_keyed_field()

    def next(self) -> Optional[dict]:
        if self.right_lookup is None:
            self._load_right()

        left = self.inputs[0]

        while True:
            if self.pending_left:
                return self.pending_left.pop(0)

            record = left.next()
            if record is None:
                return None

            key = record.get(self.key_field)
            match = self.right_lookup.get_record(key)

            if match is not None:
                merged = dict(match)
                merged.update(record)
                return merged

            elif self.mode == "outer":
                # Emit left record with missing right fields filled as None
                merged = dict(record)
                for rk in self.right_lookup.get_record(next(iter(self.right_lookup.map))).keys():
                    if rk not in merged:
                        merged[rk] = None
                return merged

            elif self.mode == "inner":
                # Skip unmatched
                continue

            elif self.mode == "left":
                # Skip unmatched
                continue
