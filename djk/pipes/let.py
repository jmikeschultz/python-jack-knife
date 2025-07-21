# Split AddField into base LetPipe and ReducePipe

from typing import Optional
from djk.base import Pipe, ParsedToken, NoBindUsage, Usage, UsageError
from djk.pipes.common import SafeNamespace
import re

# --- Shared Utilities ---
def parse_args(token: str):
    pattern = re.compile(r'^(?P<caret>\^*)(?P<field>\w+)(?P<op>[:=\+\-\*/]+)(?P<rest>.+)$')
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

# --- Evaluation ---
def do_eval(expr, env):
    try:
        return eval(expr, {}, env)
    except Exception:
        raise UsageError(f"UsageError in expression: {expr}")

def eval_regular(expr: str, record: dict):
    env = {'f': SafeNamespace(record)}

    # Special syntax: let:field:some_string → literal string assignment
    if re.match(r'^[a-zA-Z0-9_]+$', expr):
        return expr

    return do_eval(expr, env)

def eval_accumulating(expr: str, record: dict, op: str, acc=None):
    env = {'f': SafeNamespace(record)}
    if acc is not None:
        env['acc'] = acc

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
    if op == '+=':
        value = eval(expr, {}, env)
        if isinstance(value, (int, float)):
            return (acc or 0) + value
        elif isinstance(value, list):
            return (acc or []) + value
        elif isinstance(value, str):
            return str(acc or '') + value
        else:
            raise UsageError(f"Don't know how to += value of type {type(value)}")

    if op in ('-=', '*=', '/='):
        if 'acc' not in expr:
            expr = f'acc {op[0]} ({expr})'

    return do_eval(expr, env)

# --- Base Class ---
class LetPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = NoBindUsage(
            name='let',
            desc="set a new field equal to a rhs python expression"
        )
        usage.def_arg(name='rhs', usage="python rhs expression (use f.<field> syntax)")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        args = parse_args(ptok.whole_token.split(':', 1)[-1]) # NO BIND USAGE uses ptok
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']

        if self.op in ('+=', '-=', '*=', '/='):
            raise UsageError("Aggregation operator not allowed in let, use reduce:")

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        value = eval_regular(self.rest, record)
        record[self.field] = value
        return record

# --- Reduce variant ---
class ReducePipe(Pipe):
    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)
        args = parse_args(ptok.whole_token.split(':', 1)[-1])
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']

        if self.op not in ('+=', '-=', '*=', '/='):
            raise UsageError("Reduce pipe requires an accumulating operator (+=, -=, etc.)")

        self.accum_value = None

    def get_subexp_result(self):
        return (self.field, self.accum_value)

    def reset(self):
        self.accum_value = None

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        self.accum_value = eval_accumulating(self.rest, record, self.op, self.accum_value)
        return record

