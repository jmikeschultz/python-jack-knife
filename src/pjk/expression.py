# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Mike Schultz

"""Shared compilation and evaluation for record expressions."""

import ast
import json
import re
from collections.abc import Sequence

from pjk.common import ReducingNamespace, SafeNamespace
from pjk.errors import RecordProcessingError
from pjk.usage import UsageError


def _lazy_expression_value(value):
    if isinstance(value, dict):
        return ExpressionNamespace(value)
    if isinstance(value, list):
        return ExpressionSequence(value)
    return value


def materialize_expression_value(value):
    """Convert lazy expression views in an evaluated result to builtins."""
    if isinstance(value, ExpressionNamespace):
        return value._record
    if isinstance(value, ExpressionSequence):
        return value._values
    if isinstance(value, dict):
        return {
            materialize_expression_value(key): materialize_expression_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [materialize_expression_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(materialize_expression_value(item) for item in value)
    if isinstance(value, set):
        return {materialize_expression_value(item) for item in value}
    return value


class ExpressionNamespace:
    """Expose record fields lazily to avoid walking unused nested values."""

    __slots__ = ("_record",)

    def __init__(self, record):
        self._record = record

    def __getattr__(self, name):
        return _lazy_expression_value(self._record.get(name))


class ExpressionSequence(Sequence):
    """Wrap list elements only when an expression actually reads them."""

    __slots__ = ("_values",)

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


def evaluate_code(code, env, *, record: dict, token: str):
    """Evaluate compiled code with PJK's expression environment."""
    try:
        expression_env = dict(env)
        expression_env["json"] = json
        expression_env["re"] = re
        return eval(code, {}, expression_env)
    except RecordProcessingError:
        raise
    except Exception as error:
        raise RecordProcessingError(
            token=token,
            record=record,
            cause=error,
        ) from error


class CompiledExpression:
    """Parse once and evaluate consistently against PJK records."""

    def __init__(
        self,
        expr: str,
        label: str,
        reducing: bool | None = False,
        syntax_error_prefix: str = "Invalid expression",
    ):
        self.expr = expr
        self.label = label
        try:
            self.node = ast.parse(expr, mode="eval").body
            self.code = compile(ast.Expression(self.node), label, "eval")
        except SyntaxError as error:
            raise UsageError(f"{syntax_error_prefix}: {expr}") from error

        self.reducing = (
            isinstance(self.node, (ast.ListComp, ast.SetComp, ast.DictComp))
            if reducing is None
            else reducing
        )
        self.constant = self.node.value if isinstance(self.node, ast.Constant) else None
        self.is_constant = isinstance(self.node, ast.Constant)
        self.uses_f = any(
            isinstance(child, ast.Name) and child.id == "f"
            for child in ast.walk(self.node)
        )

    def evaluate(self, record: dict, *, token: str, acc=None):
        try:
            if self.is_constant:
                return self.constant

            env = {"acc": acc}
            if self.uses_f:
                namespace = (
                    ReducingNamespace if self.reducing else ExpressionNamespace
                )
                env["f"] = namespace(record)
            value = evaluate_code(
                self.code,
                env,
                record=record,
                token=token,
            )
            return materialize_expression_value(value)
        except RecordProcessingError:
            raise
        except Exception as error:
            raise RecordProcessingError(
                token=token,
                record=record,
                cause=error,
            ) from error


def eval_regular(expr: str, record: dict, *, token: str):
    """Evaluate a legacy regular expression or return its bare string."""
    if re.fullmatch(r"[a-zA-Z0-9_]+", expr):
        return expr
    return evaluate_code(
        expr,
        {"f": SafeNamespace(record)},
        record=record,
        token=token,
    )
