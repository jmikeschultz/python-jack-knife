# djk/pipes/add_field.py

from typing import Optional
from djk.base import Pipe, ParsedToken, UsageError
from djk.pipes.common import SafeNamespace
import re

def parse_args(token: str):
    pattern = re.compile(
        r'^'                            # beginning of string
        r'(?P<caret>[\^]*)'             # ^ means field added to parent document
        r'(?P<field>\w+)'               # target field
        r'(?P<op>[:=\+\/\-\*]+)'        # operator chars
        r'(?P<rest>.+)'                 # rest
        r'$'                            # end of string
    )
    match = pattern.fullmatch(token)
    if not match:
        raise ValueError(f"Invalid token syntax: {token!r}")
    return match.groupdict()

FIELD_RE = re.compile(r'\bf\.(\w+)')

class FieldProxy:
    def __init__(self, record):
        self._record = record
    def __getattr__(self, name):
        return self._record[name]

def eval_regular(expr: str, record: dict):
    env = {'f': SafeNamespace(record)}
    return do_eval(expr, env)

def eval_accumulating(expr: str, record: dict, op: str, acc=None):
    env = {'f': SafeNamespace(record)}

    # Handle list.append(...) reducer
    if expr.startswith('list.append(') and expr.endswith(')'):
        inner_expr = expr[len('list.append('):-1].strip()
        value = eval(inner_expr, {}, env)
        if isinstance(acc, list):
            return acc + [value]
        return [value] if acc is None else [acc, value]

    # Handle set.union(...) reducer
    if expr.startswith('set.union(') and expr.endswith(')'):
        inner_expr = expr[len('set.union('):-1].strip()
        value = eval(inner_expr, {}, env)
        return (acc or set()).union(value)

    # Fallback for +=, -=, etc.
    if acc is not None:
        env['acc'] = acc

        if op == '+=' and 'acc' not in expr:
            value = eval(expr, {}, env)
            if isinstance(value, (int, float)):
                return (acc or 0) + value
            elif isinstance(value, str):
                if isinstance(acc, list):
                    return acc + [value]
                return [value] if acc is None else [acc, value]
            elif isinstance(value, list):
                return (acc or []) + value
            else:
                raise UsageError(f"Don't know how to += value of type {type(value)}")

        if op in ('-=', '*=', '/=') and 'acc' not in expr:
            expr = f'acc {op[0]} ({expr})'

    return do_eval(expr, env)

def do_eval(expr, env):
    try:
        return eval(expr, {}, env)
    except UsageError as e:
        lineno = e.lineno or 1
        offset = e.offset or 0

        # Build caret line for error pointer
        lines = expr.splitlines()
        error_line = lines[lineno - 1] if lineno - 1 < len(lines) else expr

        error_msg = (
            f"UsageError in expression: {error_line}"
        )
        raise UsageError(error_msg) from None

class AddField(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)

        # non standard from other pipes, we parse everything
        arg_string = ptok.whole_token.split(':', 1)[-1]

        args = parse_args(arg_string)
        self.field = args.get('field')
        self.op = args.get('op')
        self.rest = args.get('rest')
        self.count = 0

        ACCUM_OPS = ('+=', '-=', '*=', '/=')
        ACCUM_FUNCS = ('list.append(', 'set.union(', 'dict.update(')

        is_acc_function = (
            self.op in ACCUM_OPS
            or any(self.rest.strip().startswith(f) for f in ACCUM_FUNCS)
        )
        for_parent = len(args.get('caret')) == 1

        if for_parent:
            if not is_acc_function:
                raise ValueError('only accumulating expressions (e.g. +=) are assignable to a parent field (^field)')
        else:
            if is_acc_function:
                raise ValueError('accumulating expressions (e.g +=) must be assigned to a parent field (^field)')

        self.is_accumulating = for_parent
        self.accum_value = None
        self.accum_field = self.field if self.is_accumulating else None

        '''
        examples:

        add:field:mystring             # literal string
        add:field1=f.field2+10         # assign field1 value of expression
        add:^field1+=f.field           # accumulate to parent
        add:^items=list.append(f.id)   # collect into parent list
        add:^tags=set.union({f.type})  # collect into parent set
        '''

    def get_subexp_result(self):
        return (self.accum_field, self.accum_value)
    
    def reset(self):
        self.accum_value = None

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        self.count += 1

        return self.reducing_next(record) if self.is_accumulating else self.regular_next(record)

    def regular_next(self, record):
        if isinstance(record, dict):
            field_value = self.rest if ':' in self.op else eval_regular(self.rest, record)
            record[self.field] = field_value
            return record
        
        # scalar, this can for [ over:foo where foo = list[int,etc] OR pjk '[0, 1]' where records are scalars
        else: 
            scalar = record
            # special syntax where _ means 'the scalar'
            # e.g. add:foo:_ --> returns {foo: <the_scalar>}
            # e.g. add:foo=f._ + 10 ...
            if '_' not in self.rest:
                raise('Only add:name:_ or add:name:f._ +,-,/,* 100 allowed')

            if ':' not in self.op:
                rec = {f'_': scalar} # 
                field_value = eval_regular(self.rest, rec)
                return {self.field: field_value}

            else:
                return {self.field: scalar}

    def reducing_next(self, record):
        if ':' in self.op:
            raise UsageError("Literal assignment to parent makes no sense")
        self.accum_value = eval_accumulating(self.rest, record, self.op, self.accum_value)
        return record
