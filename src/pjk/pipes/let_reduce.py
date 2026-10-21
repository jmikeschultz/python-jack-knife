# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/let_reduce.py

from pjk.components import DeepCopyPipe
from pjk.errors import RecordProcessingError
from pjk.usage import ParsedToken, Usage, UsageError, TokenError, NoBindUsage
from pjk.common import SafeNamespace, ReducingNamespace
from collections.abc import Sequence
import math
import re
import ast
import json

# --- Shared Utilities ---
def _record_path(record, path):
    value = record
    for part in path:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = getattr(value, part, None)
        if value is None:
            return None
    return value


def _lazy_expression_value(value):
    if isinstance(value, dict):
        return ExpressionNamespace(value)
    if isinstance(value, list):
        return ExpressionSequence(value)
    return value


def _materialize_expression_value(value):
    """Convert lazy expression views in an evaluated result to builtins."""
    if isinstance(value, ExpressionNamespace):
        return value._record
    if isinstance(value, ExpressionSequence):
        return value._values
    if isinstance(value, dict):
        return {
            _materialize_expression_value(key): _materialize_expression_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_materialize_expression_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_materialize_expression_value(item) for item in value)
    if isinstance(value, set):
        return {_materialize_expression_value(item) for item in value}
    return value


class ExpressionNamespace:
    """Expose record fields lazily to avoid walking unused nested values."""

    __slots__ = ('_record',)

    def __init__(self, record):
        self._record = record

    def __getattr__(self, name):
        return _lazy_expression_value(self._record.get(name))


class ExpressionSequence(Sequence):
    """Wrap list elements only when an expression actually reads them."""

    __slots__ = ('_values',)

    def __init__(self, values):
        self._values = values

    def __len__(self):
        return len(self._values)

    def __getitem__(self, index):
        value = self._values[index]
        if isinstance(index, slice):
            return [_lazy_expression_value(item) for item in value]
        return _lazy_expression_value(value)

    def __iter__(self):
        return (_lazy_expression_value(value) for value in self._values)

    def __repr__(self):
        return repr(list(self))

    def __eq__(self, other):
        if isinstance(other, ExpressionSequence):
            other = other._values
        return self._values == other

    def __add__(self, other):
        return list(self) + list(other)

    def __radd__(self, other):
        return list(other) + list(self)


class PreparedExpression:
    """Parse once and cheaply evaluate common record expressions."""

    def __init__(self, expr: str, label: str, reducing: bool | None = False):
        self.expr = expr
        self.label = label
        try:
            self.node = ast.parse(expr, mode='eval').body
            self.code = compile(ast.Expression(self.node), label, 'eval')
        except SyntaxError as e:
            raise UsageError(f"Invalid expression: {expr}") from e

        self.reducing = (
            isinstance(self.node, (ast.ListComp, ast.SetComp, ast.DictComp))
            if reducing is None
            else reducing
        )

        self.constant = self.node.value if isinstance(self.node, ast.Constant) else None
        self.is_constant = isinstance(self.node, ast.Constant)
        self.uses_f = any(
            isinstance(child, ast.Name) and child.id == 'f'
            for child in ast.walk(self.node)
        )

    def evaluate(self, record: dict, *, token: str, acc=None):
        try:
            if self.is_constant:
                return self.constant

            env = {'acc': acc}
            if self.uses_f:
                namespace = ReducingNamespace if self.reducing else ExpressionNamespace
                env['f'] = namespace(record)
            value = do_eval(self.code, env, record=record, token=token)
            return _materialize_expression_value(value)
        except RecordProcessingError:
            raise
        except Exception as e:
            raise RecordProcessingError(token=token, record=record, cause=e) from e


def parse_args(token: str):
    pattern = re.compile(r'(?P<field>\w+)(?P<op>[:=\+\-\*/]+)(?P<rest>.+)$')
    match = pattern.fullmatch(token)
    if not match:
        raise ValueError(f"Invalid token syntax: {token!r}")
    return match.groupdict()

def do_eval(expr, env, *, record: dict, token: str):
    try:
        safe_env = dict(env)
        safe_env['json'] = json
        safe_env['re'] = re
        return eval(expr, {}, safe_env)
    except RecordProcessingError:
        raise
    except Exception as e:
        raise RecordProcessingError(
            token=token,
            record=record,
            cause=e,
        ) from e

def eval_regular(expr: str, record: dict, *, token: str):
    env = {'f': SafeNamespace(record)}
    if re.match(r'[a-zA-Z0-9_]+$', expr):
        return expr
    return do_eval(expr, env, record=record, token=token)

def accumulate_value(value, node, op: str, acc=None):

    if isinstance(node, ast.ListComp):
        values = value
        return (acc or []) + list(values)

    if isinstance(node, ast.SetComp):
        values = value
        return (acc or set()).union(values)

    if isinstance(node, ast.DictComp):
        values = value
        return {**(acc or {}), **values}

    if op == '+=':
        if isinstance(value, (int, float)):
            return (acc or 0) + value
        elif isinstance(value, str):
            return str(acc or '') + value
        elif isinstance(value, list):
            return (acc or []) + value
        else:
            return (acc or []) + [value]

    return value

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
        self.prepared = None
        self.literal_value = None

        if self.op in ('+=', '-=', '*=', '/='):
            raise TokenError("Aggregation operator not allowed in let, use reduce:")
        if self.op != ':' and re.fullmatch(r'[a-zA-Z0-9_]+', self.rest):
            self.literal_value = self.rest
        elif self.op != ':':
            self.prepared = PreparedExpression(self.rest, '<let>')

    def reset(self):
        pass  # stateless

    def __iter__(self):
        token = self.ptok.whole_token
        for record in self.left:
            try:
                if self.op == ':':
                    record[self.field] = self.rest
                elif self.literal_value is not None:
                    record[self.field] = self.literal_value
                else:
                    record[self.field] = self.prepared.evaluate(record, token=token)
            except RecordProcessingError:
                raise
            except Exception as e:
                raise RecordProcessingError(token=token, record=record, cause=e) from e
            yield record

# --- Named aggregations (ave, sum, min, max) ---
# Match agg:f.field or agg:f.field.subfield (must use f. prefix)
NAMED_AGG_PATTERN = re.compile(r'^(\w+):f\.([\w.]+)$')
NAMED_AGGS = frozenset(('ave', 'sum', 'min', 'max', 'p50', 'p75', 'p90'))
PERCENTILE_MAP = {'p50': 50, 'p75': 75, 'p90': 90}


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
    raw_val = _record_path(record, field_path.split('.'))

    val = _to_number(raw_val)

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
    elif agg_name in PERCENTILE_MAP:
        if val is not None:
            lst = acc if acc is not None else []
            lst.append(val)
            return lst
    return acc


def _percentile(sorted_vals: list, p: float):
    """Nearest-rank: returns the actual element at the percentile position (no interpolation)."""
    if not sorted_vals:
        return 0
    n = len(sorted_vals)
    rank = math.ceil(p / 100 * n)
    idx = min(max(0, rank - 1), n - 1)
    return sorted_vals[idx]


def finalize_named_agg(agg_name: str, acc):
    if agg_name == 'ave':
        s, n = acc or (0, 0)
        return s / n if n else 0
    if agg_name in PERCENTILE_MAP:
        vals = acc or []
        if not vals:
            return 0
        vals.sort()
        return _percentile(vals, PERCENTILE_MAP[agg_name])
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
            "or use list/dict comprehension, or named agg: ave:f.field, sum:f.field, p50:f.field, etc. (supports f.field.subfield)",
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

        usage.def_example(expr_tokens=["[{x:1},{x:2},{x:3},{x:4}]",
                                       'reduce:p75=p75:f.x'
                                       ],
                        expect="{p75: 3}")

        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        args = parse_args(ptok.whole_token.split(':', 1)[-1])
        self.field = args['field']
        self.op = args['op']
        self.rest = args['rest']
        self.named_agg = None
        self.prepared = None
        self.prepared_node = None

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

        if not self.named_agg:
            expr = self.rest
            if self.op in ('-=', '*=', '/=') and 'acc' not in expr:
                expr = f'acc {self.op[0]} ({expr})'
            self.prepared = PreparedExpression(
                expr,
                '<reduce>',
                reducing=None,
            )
            self.prepared_node = self.prepared.node

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
        token = self.ptok.whole_token
        for record in self.left:
            try:
                if self.named_agg:
                    agg_name, field_name = self.named_agg
                    self.accum_value = eval_named_aggregation(agg_name, field_name, record, self.accum_value)
                else:
                    value = self.prepared.evaluate(
                        record,
                        token=token,
                        acc=self.accum_value,
                    )
                    self.accum_value = accumulate_value(
                        value,
                        self.prepared_node,
                        self.op,
                        self.accum_value,
                    )
            except RecordProcessingError:
                raise
            except Exception as e:
                raise RecordProcessingError(token=token, record=record, cause=e) from e
            yield record

    def get_subexp_result(self):
        if self.named_agg:
            agg_name, _ = self.named_agg
            value = finalize_named_agg(agg_name, self.accum_value)
            return (self.field, value)
        return (self.field, self.accum_value)
