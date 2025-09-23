# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Any, List, Callable
import os
import shlex
from typing import Optional, Any, List
from pjk.base import Source, Pipe, Sink, TokenError, UsageError, ParsedToken, Usage
from pjk.pipes.user_pipe_factory import UserPipeFactory
from pjk.pipes.let_reduce import ReducePipe
from pjk.registry import ComponentRegistry

def expand_macros(tokens: List[str]) -> List[str]:
    expanded = []
    for token in tokens:
        if token.endswith(".pjk"):
            if not os.path.isfile(token):
                raise FileNotFoundError(f"Macro file not found: {token}")
            with open(token, "r") as f:
                lines = f.readlines()

            # Remove comments outside quotes, then split
            stripped = []
            for line in lines:
                try:
                    parts = shlex.split(line, comments=True, posix=True)
                    stripped.extend(parts)
                except ValueError as e:
                    raise UsageError(f"Error parsing {token}: {e}")
            expanded.extend(stripped)
        else:
            expanded.append(token)
    return expanded

class ExpressionParser:
    def __init__(self, registry: ComponentRegistry):
        self.stack: List[Any] = []
        self.registry = registry

    def get_sink(self, stack_helper, token):
        if len(self.stack) < 1:
            raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

        source = self.stack.pop()
        if len(self.stack) != 0:
            raise TokenError.from_list(['A sink can only consume one source.',
                                        'pjk <source> [<pipe> ...] <sink>'])

        # if there's top level aggregation for reduction
        aggregator = stack_helper.get_reducer_aggregator()
        if aggregator:
            aggregator.add_source(source)
            source = aggregator

        sink = self.registry.create_sink(token)
        
        if not sink:
            raise TokenError.from_list(['expression must end in a sink.',
                            'pjk <source> [<pipe> ...] <sink>'])

        sink.add_source(source)
        return sink

    def parse(self, tokens: List[str]) -> Sink:
        self.tokens = expand_macros(tokens)
        usage_error_message = "You've got a problem here."
        stack_helper = StackLoader()
        pos = 0
        
        try:
            if len(self.tokens) < 2:
                raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

            for pos, token in enumerate(self.tokens):
                if pos == len(self.tokens) - 1: # token should be THE sink
                    return self.get_sink(stack_helper, token)
                    
                source = self.registry.create_source(token)
                if source:
                    stack_helper.add_operator(source, self.stack)
                    continue

                subexp = SubExpression.create(token)
                if subexp:
                    stack_helper.add_operator(subexp, self.stack)
                    continue

                pipe = self.registry.create_pipe(token)
                if pipe:
                    stack_helper.add_operator(pipe, self.stack)
                    continue

                else: # unrecognized token
                    # could be sink in WRONG position, let's see for better error message
                    sink = self.registry.create_sink(token) 
                    if sink:
                        raise TokenError.from_list(['sink may only occur in final position.',
                                            'pjk <source> [<pipe> ...] <sink>'])
                    raise TokenError.from_list([token, 'unrecognized token'])
        
        except TokenError as e:
            raise UsageError(usage_error_message, self.tokens, pos, e)
    
class ReducerAggregatorPipe(Pipe):
    def __init__(self, top_level_reducers: List[Any]):
        super().__init__(None)
        self.top_level_reducers = top_level_reducers
        self.reduction = {}
        self.done = False

    def reset(self):
        self.done = False
        self.reduction.clear()

    def __iter__(self):
        if not self.done:
            for _ in self.left:
                pass  # consume all input
            for reducer in self.top_level_reducers:
                name, value = reducer.get_subexp_result()
                self.reduction[name] = value
            self.done = True
            yield self.reduction

class StackLoader:
    def __init__(self):
        self.top_level_reducers = []

    def get_reducer_aggregator(self) -> ReducerAggregatorPipe:
        if not self.top_level_reducers:
            return None
        
        return ReducerAggregatorPipe(top_level_reducers=self.top_level_reducers)

    def add_operator(self, op, stack):
        if len(stack) > 0 and isinstance(stack[-1], Pipe):
            target = stack[-1]

            if isinstance(target, SubExpression):
                if isinstance(op, SubExpressionOver):
                    subexp_begin = stack.pop()
                    subexp_begin.set_over_arg(op.get_over_arg())
                    op.add_source(subexp_begin)
                    stack.append(op)
                    return
                else: # an operator within the subexpression
                    target.add_subop(op)
                    return

        # order matters, sources are pipes
        if isinstance(op, Pipe):
            arity = op.arity # class level attribute
            if len(stack) < arity:
                raise UsageError(f"'{op}' requires {arity} input(s)")
            for _ in range(arity):
                op.add_source(stack.pop())
            stack.append(op)

            if isinstance(op, ReducePipe):
                self.top_level_reducers.append(op)

            return

        elif isinstance(op, Source):
            stack.append(op)
            return
            
# special upstream source put in subexp stack for flexibility
# when we don't know what that upstream source will be.
class UpstreamSource(Source):
    def __init__(self):
        self.data = []
        self.inner_source = None

    def set_source(self, source: Source):
        self.inner_source = source

    def set_list(self, items):
        self.data = items if items else []

    def add_item(self, rec):
        self.data.append(rec)

    def reset(self):
        # nothing needed in generator model
        pass

    def __iter__(self):
        if self.inner_source:
            yield from self.inner_source
        else:
            for item in self.data:
                yield item
    
class SubExpression(Pipe):
    @classmethod
    def create(cls, token: str) -> Pipe:
        ptok = ParsedToken(token)
        if ptok.pre_colon == '[':
            return SubExpression(ptok, None)
        if ptok.pre_colon == 'over':
            return SubExpressionOver(ptok, None)
        return None

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.upstream_source = UpstreamSource()
        self.over_arg = None
        self.over_field = None
        self.subexp_stack = [self.upstream_source]
        self.subexp_ops = []
        self.over_pipe = None
        self.stack_helper = StackLoader()

    def add_subop(self, op):
        self.subexp_ops.append(op)
        self.stack_helper.add_operator(op, self.subexp_stack)

    def set_over_arg(self, over_arg):
        self.over_arg = over_arg
        if over_arg.endswith('.py'):
            self.over_field = 'child'
            self.over_pipe = UserPipeFactory.create(over_arg)
            self.upstream_source.set_source(self.over_pipe)
            self.subexp_ops.append(self.over_pipe)
        else:
            self.over_field = over_arg

    def reset(self):
        for op in self.subexp_ops:
            if isinstance(op, Pipe):
                op.reset()

    def __iter__(self):
        for record in self.left:
            if self.over_pipe:
                one = UpstreamSource()
                one.add_item(record)
                self.over_pipe.set_sources([one])
            else:
                field_data = record.pop(self.over_field, None)
                if not field_data:
                    yield record
                    continue
                if isinstance(field_data, list):
                    self.upstream_source.set_list(field_data)
                else:
                    self.upstream_source.set_list([field_data])

            # Reset sub-pipe stack
            for op in self.subexp_ops:
                op.reset()

            out_recs = []
            for rec in self.subexp_stack[-1]:
                out_recs.append(rec)

            record[self.over_field] = out_recs

            for op in self.subexp_ops:
                get_subexp = getattr(op, "get_subexp_result", None)
                if get_subexp:
                    name, value = get_subexp()
                    if name:
                        record[name] = value

            yield record

class SubExpressionOver(Pipe):
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.over_arg = ptok.get_arg(0)

    def get_over_arg(self):
        return self.over_arg

    def reset(self):
        pass  # stateless

    def __iter__(self):
        yield from self.left

