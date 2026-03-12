# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/let_reduce.py

from pjk.components import DeepCopyPipe
from pjk.usage import ParsedToken, Usage, UsageError, TokenError, NoBindUsage
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
        safe_env['re'] = re
        return eval(expr, {}, safe_env)
    except Exception:
        raise Exception(f"Error in expression: {expr}")

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
class LetPipe(DeepCopyPipe):
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
        super().__init__(ptok, usage)
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

# --- Named aggregations (ave, sum, min, max) ---
# Match agg:f.field or agg:f.field.subfield (must use f. prefix)
NAMED_AGG_PATTERN = re.compile(r'^(\w+):f\.([\w.]+)$')
NAMED_AGGS = frozenset(('ave', 'sum', 'min', 'max'))


def _get_nested(ns, path: str):
    """Resolve dot path (e.g. 'size' or 'address.zip') against SafeNamespace."""
    for part in path.split('.'):
        ns = getattr(ns, part, None)
        if ns is None:
            return None
    return ns


def _to_number(val):
    """Coerce to int or float; returns None if not coercible."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    return None


def eval_named_aggregation(agg_name: str, field_path: str, record: dict, acc):
    ns = SafeNamespace(record)
    val = _to_number(_get_nested(ns, field_path))

    if agg_name == 'ave':
        s, n = acc or (0, 0)
        if val is not None:
            return (s + val, n + 1)
        return (s, n)
    elif agg_name == 'sum':
        base = acc if acc is not None else 0
        if val is not None:
            return base + val
        return base
    elif agg_name == 'min':
        if val is None:
            return acc
        if acc is None:
            return val
        return min(acc, val)
    elif agg_name == 'max':
        if val is None:
            return acc
        if acc is None:
            return val
        return max(acc, val)
    return acc


def finalize_named_agg(agg_name: str, acc):
    if agg_name == 'ave':
        s, n = acc or (0, 0)
        return s / n if n else 0
    return acc


# --- ReducePipe (stateful accumulator) ---
def is_comprehension(expr: str) -> bool:
    try:
        node = ast.parse(expr, mode='eval').body
        return isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp))
    except SyntaxError:
        return False

class ReducePipe(DeepCopyPipe):
    @classmethod
    def usage(cls):
        usage = NoBindUsage( # can't use bound usage because of complicated parsing
            name='reduce',
            desc="set a new field equal to a reduction over records of a sub or main expression\n" +
            "rhs operators must be accumulating, e.g. +=, -=, *=, /=\n" +
            "or use list/dict comprehension, or named agg: ave:f.field, sum:f.field, etc. (supports f.field.subfield)",
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

        usage.def_example(expr_tokens=["{cars:[{size:9}, {size:4}]}",
                                       '[', 'reduce:ave_size=ave:f.size', 'over:cars'
                                       ],
                        expect="{cars:[{size:9}, {size:4}], ave_size: 6.5}")

        usage.def_example(expr_tokens=["{cars:[{size:9}, {size:4}]}",
                                       '[', 'reduce:min_size=min:f.size', 'over:cars'
                                       ],
                        expect="{cars:[{size:9}, {size:4}], min_size: 4}")

        usage.def_example(expr_tokens=["{cars:[{size:9}, {size:4}]}",
                                       '[', 'reduce:max_size=max:f.size', 'over:cars'
                                       ],
                        expect="{cars:[{size:9}, {size:4}], max_size: 9}")

        usage.def_example(expr_tokens=["{items:[{metrics:{size:10}}, {metrics:{size:3}}]}",
                                       '[', 'reduce:ave_size=ave:f.metrics.size', 'over:items'
                                       ],
                        expect="{items:[{metrics:{size:10}}, {metrics:{size:3}}], ave_size: 6.5}")


        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        args = parse_args(ptok.whole_token.split(':', 1)[-1])
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']
        self.named_agg = None

        if self.op == '=':
            m = NAMED_AGG_PATTERN.match(self.rest)
            if m and m.group(1) in NAMED_AGGS:
                self.named_agg = (m.group(1), m.group(2))
            elif is_comprehension(self.rest):
                self.op = '+='
            else:
                raise TokenError("Named aggregation requires f. prefix (e.g. ave:f.size, ave:f.field.subfield)")
        elif self.op not in ('+=', '-=', '*=', '/='):
            if is_comprehension(self.rest):
                self.op = '+='
            else:
                raise TokenError("Reduce pipe requires an accumulating operator (+=, -=, etc.), unless RHS is a comprehension or named agg (ave:f.field, sum:f.field, etc.)")

        self.accum_value = self.initial_acc_value()

    def initial_acc_value(self):
        if self.named_agg:
            return None
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
            if self.named_agg:
                agg_name, field_name = self.named_agg
                self.accum_value = eval_named_aggregation(agg_name, field_name, record, self.accum_value)
            else:
                self.accum_value = eval_accumulating(self.rest, record, self.op, self.accum_value)
            yield record

    def get_subexp_result(self):
        if self.named_agg:
            agg_name, _ = self.named_agg
            value = finalize_named_agg(agg_name, self.accum_value)
            return (self.field, value)
        return (self.field, self.accum_value)
