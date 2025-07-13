# djk/pipes/grep.py

import re
from typing import Optional
from djk.base import Pipe, ParsedToken, SyntaxError

import operator

ops = {
    '=': operator.eq,
    '==': operator.eq,
    '!=': operator.ne,
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge,
}

def split_on_operator(s):
    # Order matters: match >= and <= before > and <
    pattern = r'(<=|>=|!=|=|<|>|~)'
    match = re.split(pattern, s, maxsplit=1)
    if len(match) == 3:
        lhs, op, rhs = match
        return lhs.strip(), op, rhs.strip()
    else:
        raise ValueError(f"Invalid expression: {s}")

class GrepPipe(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)

        arg_string = ptok.whole_token.split(':')[-1]
        try:
            self.field, op_str, self.rhs = split_on_operator(arg_string)
            
            # tilde is for regex (yay Perl!)
            if '~' in op_str:
                self.pattern = re.compile(self.rhs)
                self.op = None
            else:
                self.pattern = None
                self.op = ops[op_str]
                if not self.op:
                    raise SyntaxError(f'bad operator: {op_str}')
                self.number = int(self.rhs)
                
        except Exception:
            raise SyntaxError("grep:field:regex requires a valid field and regex (fixme)")

    def next(self) -> Optional[dict]:
        while True:
            record = self.inputs[0].next()
            if record is None:
                return None
            value = record.get(self.field)
            if isinstance(value, str) and self.pattern.search(value):
                return record
            elif (isinstance(value, int) or isinstance(value, float)):
                if self.op(value, self.number):
                    return record

            # skip this record and continue looping
