# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/let_reduce.py

from pjk.base import Pipe, ParsedToken, NoBindUsage, Usage, UsageError, TokenError
from pjk.common import SafeNamespace, ReducingNamespace
import re
import ast
import json

# --- Shared Utilities ---
def parse_args(token: str):
    pattern = re.compile(r'(?P<field>\w+)(?P<op>[:=\+\-\*/]+)(?P<rest>.+)$')
    match = pattern.fullmatch(token)
    if not match:
        raise ValueError(f"Invalid token syntax: {token!r}")
    return match.groupdict()

def do_eval(expr, env):
    try:
        safe_env = dict(env)
        safe_env['json'] = json
        return eval(expr, {}, safe_env)
    except Exception:
        raise UsageError(f"UsageError in expression: {expr}")

def eval_regular(expr: str, record: dict):
    env = {'f': SafeNamespace(record)}
    if re.match(r'[a-zA-Z0-9_]+$', expr):
        return expr
    return do_eval(expr, env)

def eval_accumulating(expr: str, record: dict, op: str, acc=None):
    if op in ('-=', '*=', '/=') and 'acc' not in expr:
        expr = f'acc {op[0]} ({expr})'

    env = {
        'f': SafeNamespace(record),
        'acc': acc
    }

    try:
        node = ast.parse(expr, mode='eval').body
    except SyntaxError:
        raise UsageError(f"Invalid expression: {expr}")

    if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp)):
        env['f'] = ReducingNamespace(record)

    if isinstance(node, ast.ListComp):
        values = eval(compile(ast.Expression(node), '<reduce:listcomp>', 'eval'), {}, env)
        return (acc or []) + list(values)

    if isinstance(node, ast.SetComp):
        values = eval(compile(ast.Expression(node), '<reduce:setcomp>', 'eval'), {}, env)
        return (acc or set()).union(values)

    if isinstance(node, ast.DictComp):
        values = eval(compile(ast.Expression(node), '<reduce:dictcomp>', 'eval'), {}, env)
        return {**(acc or {}), **values}

    if op == '+=':
        value = eval(expr, {}, env)
        if isinstance(value, (int, float)):
            return (acc or 0) + value
        elif isinstance(value, str):
            return str(acc or '') + value
        elif isinstance(value, list):
            return (acc or []) + value
        else:
            return (acc or []) + [value]

    if op in ('-=', '*=', '/='):
        return do_eval(expr, env)

    return do_eval(expr, env)

# --- LetPipe (simple field assignment) ---
class LetPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = NoBindUsage( # can't use bound usage because of complicated parsing
            name='let',
            desc="set a new field equal to a rhs python expression",
            component_class=cls
        )
        usage.def_arg(name='rhs', usage="python rhs expression (use f.<field> syntax)")
        usage.def_example(expr_tokens=['{hello:0}', 'let:there=f.hello + 1'], expect="{hello:0, there: 1}")
        usage.def_example(expr_tokens=['{hello:0}', 'let:foo:bar'], expect="{hello:0, foo: 'bar'}")
        usage.def_example(expr_tokens=['{hello:0}', 'let:foo=int(1)'], expect="{hello:0, foo: 1}")        
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        args = parse_args(ptok.whole_token.split(':', 1)[-1])
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']

        if self.op in ('+=', '-=', '*=', '/='):
            raise TokenError("Aggregation operator not allowed in let, use reduce:")

    def reset(self):
        pass  # stateless

    def __iter__(self):
        for record in self.left:
            if self.op == ':':
                record[self.field] = self.rest
            else:
                record[self.field] = eval_regular(self.rest, record)
            yield record

# --- ReducePipe (stateful accumulator) ---
def is_comprehension(expr: str) -> bool:
    try:
        node = ast.parse(expr, mode='eval').body
        return isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp))
    except SyntaxError:
        return False

class ReducePipe(Pipe):
    @classmethod
    def usage(cls):
        usage = NoBindUsage( # can't use bound usage because of complicated parsing
            name='reduce',
            desc="set a new field equal to a reduction over records of a sub or main expression\n" +
            "rhs operators must be accumulating, e.g. +=, -=, *=, /=\n" +
            "or use list or dictionary comprehension",
            component_class=cls
        )
        usage.def_arg(name='rhs', usage="accumulating python rhs expression (use f.<field> syntax)")

        usage.def_example(expr_tokens=["{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}",
                                       '[', 'reduce:total_size+=f.size', 'over:cars'
                                       ],
                        expect="{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}], total_size: 13}")
        
        usage.def_example(expr_tokens=["[{make: 'honda'}, {make: 'ford'}, {make:'bmw'}]",
                                       'reduce:cars=[x for x in f.make]'
                                       ],
                        expect="{cars:['honda', 'ford', 'bmw']}")
        
        usage.def_example(expr_tokens=["[{i:[1,2]},{i:[3]}]",
                                       'reduce:flattened=[x for x in f.i]'
                                       ],
                        expect="{flattened:[1, 2, 3]}")
        
        usage.def_example(expr_tokens=["[{i:1},{i:3}, {i:7}]",
                                       'reduce:diff-=f.i'
                                       ],
                        expect="{diff:-11}")
        
        usage.def_example(expr_tokens=["[{i:1},{i:3}, {i:7}]",
                                       'reduce:product*=f.i'
                                       ],
                        expect="{product:21}")

        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        args = parse_args(ptok.whole_token.split(':', 1)[-1])
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']

        if self.op not in ('+=', '-=', '*=', '/='):
            if is_comprehension(self.rest):
                self.op = '+='
            else:
                raise TokenError("Reduce pipe requires an accumulating operator (+=, -=, etc.), unless RHS is a comprehension")

        self.accum_value = self.initial_acc_value()

    def initial_acc_value(self):
        if self.op == '+=':
            return 0
        elif self.op == '*=':
            return 1
        elif self.op == '-=':
            return 0
        elif self.op == '/=':
            return 1.0
        else:
            return None

    def reset(self):
        self.accum_value = self.initial_acc_value()

    def __iter__(self):
        for record in self.left:
            self.accum_value = eval_accumulating(self.rest, record, self.op, self.accum_value)
            yield record

    def get_subexp_result(self):
        return (self.field, self.accum_value)
