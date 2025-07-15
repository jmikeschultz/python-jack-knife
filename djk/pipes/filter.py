from typing import Optional
from djk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource

class FilterPipe(Pipe):
    arity = 2  # left = record stream, right = keyed source

    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)

        # not regular parsing
        arg_string = ptok.whole_token.split(':')[-1]
        if arg_string not in ("+", "-"):
            raise UsageError("filter:<arg> must be '+' or '-'")

        self.mode = arg_string  # '+' = include if matched, '-' = exclude if matched
        self.key_field = None
        self.right_lookup = None

    def _load_right(self):
        right = self.inputs[1]
        if not isinstance(right, KeyedSource):
            raise UsageError("Right input to filter must be a KeyedSource")
        self.right_lookup = right
        self.key_field = right.get_keyed_field()

    def next(self) -> Optional[dict]:
        if self.right_lookup is None:
            self._load_right()

        left = self.inputs[0]

        while True:
            record = left.next()
            if record is None:
                return None

            key = record.get(self.key_field)
            if key is None:
                continue  # skip records without a key

            exists = self.right_lookup.get_record(key) is not None
            if (self.mode == "+" and exists) or (self.mode == "-" and not exists):
                return record
