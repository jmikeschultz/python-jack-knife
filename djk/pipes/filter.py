from typing import Optional
from djk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource

class FilterPipe(Pipe):
    arity = 2  # left = record stream, right = keyed source

    @classmethod
    def usage(cls):
        usage = Usage(
            name="filter",
            desc="Filters left records based on presence in right keyed source"
        )
        usage.def_arg("mode", "'+' to include matches, '-' to exclude matches",
                      valid_values={'+', '-'})
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.mode = usage.get_arg('mode')

        '''
        arg_string = ptok.get_arg(0)
        if arg_string not in ("+", "-"):
            raise UsageError(
                "filter:<mode> must be '+' or '-'",
                tokens=[ptok.whole_token],
                token_no=0,
                details={"received": arg_string}
            )
'''
        self.right = None
        self.left = None

    def _setup(self):
        right = self.inputs[1]
        if not isinstance(right, KeyedSource):
            raise UsageError("Right input to filter must be a KeyedSource")
        self.right = right
        self.left = self.inputs[0]

    def next(self) -> Optional[dict]:
        if self.right is None:
            self._setup()

        while True:
            record = self.left.next()
            if record is None:
                return None

            match = self.right.lookup(record)
            exists = match is not None

            if (self.mode == "+" and exists) or (self.mode == "-" and not exists):
                return record
            # otherwise, keep looping
