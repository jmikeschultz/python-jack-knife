# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Any, List, Callable
import os
import shlex
from typing import Optional, Any, List
from djk.base import Source, Pipe, Sink, TokenError, UsageError, ParsedToken, Usage
from djk.pipes.factory import PipeFactory
from djk.pipes.user_pipe_factory import UserPipeFactory
from djk.pipes.let_reduce import ReducePipe
from djk.sinks.factory import SinkFactory
from djk.sources.factory import SourceFactory

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
    def __init__(self, tokens: List[str]):
        self.tokens = expand_macros(tokens)
        self.stack: List[Any] = []

    def parse(self) -> Sink:
        usage_error_message = "You've got a problem here."
        stack_helper = StackLoader()
        pos = 0
        
        try:
            if len(self.tokens) < 2:
                raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

            for pos, token in enumerate(self.tokens):
                if pos == len(self.tokens) - 1: # should be sink
                    if len(self.stack) > 0:
                        penult = self.stack.pop()

                        # if there's top level aggregation
                        aggregator = stack_helper.get_reducer_aggregator()
                        if aggregator:
                            aggregator.set_sources([penult])
                            sink = SinkFactory.create(token, aggregator)
                        else:
                            sink = SinkFactory.create(token, penult)

                        if not sink:
                            raise TokenError.from_list(['expression must end in a sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])
                        
                    if len(self.stack) != 0:
                        raise TokenError.from_list(['A sink can only consume one source.',
                                                    'pjk <source> [<pipe> ...] <sink>'])
                    
                    return sink

                source = SourceFactory.create(token)
                if source:
                    stack_helper.add_operator(source, self.stack)
                    continue

                pipe = SubExpression.create(token)
                if pipe:
                    stack_helper.add_operator(pipe, self.stack)
                    continue

                pipe = PipeFactory.create(token)
                if pipe:
                    stack_helper.add_operator(pipe, self.stack)
                    continue

                else: # unrecognized token
                    # could be sink in WRONG position, let's see for better error message
                    sink = SinkFactory.create(token, None) 
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

    def next(self) -> Optional[dict]:
        while not self.done:
            record = self.inputs[0].next()
            if record is None:
                break
        
        if not self.done:
            for reducer in self.top_level_reducers:
                name, value = reducer.get_subexp_result()
                self.reduction[name] = value
            self.done = True
        else:
            return None
        
        return self.reduction

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
                    op.set_sources([subexp_begin])
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
            op.set_sources([stack.pop() for _ in range(arity)][::-1])
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
        self.index = 0
        self.inner_source = None

    def set_source(self, source: Source):
        self.inner_source = source

    def set_list(self, items):
        self.index = 0
        self.data = items if items else []

    def add_item(self, rec):
        self.data.append(rec)

    def reset(self):
        self.data = []
    
    def next(self) -> Optional[dict]:
        if self.inner_source:
            return self.inner_source.next()

        if self.index >= len(self.data):
            return None
        item = self.data[self.index]
        self.index += 1
        return item
    
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
        self.over_arg = None # set by method
        self.over_field = None
        self.subexp_stack: List[Any] = [self.upstream_source] # put list source on operand stack
        self.subexp_ops: List[Any] = [] # list of operators for parent to reset
        self.over_pipe = None
        self.stack_helper = StackLoader()
        
    def add_subop(self, op):
        self.subexp_ops.append(op)
        self.stack_helper.add_operator(op, self.subexp_stack)

    # over_arg can be either a field name (expecting a list)
    # or it can be a 1-to-many user_pipe
    # need to think on this, if it makes sense to allow pipes in general
    # the a one-to-many pipe seems very natural e.g. 1 query in -> many results out
    # and the subexpress can operate on the results stream into the 'child' field
    def set_over_arg(self, over_arg):
        self.over_arg = over_arg
        if over_arg.endswith('.py'):
            self.over_field = 'child' # where the new subrecs from the over_pipe will go, default as 'child' field
            self.over_pipe = UserPipeFactory.create(over_arg)
            self.upstream_source.set_source(self.over_pipe) # self.upstream_source already on stack
            self.subexp_ops.append(self.over_pipe) # so the pipe gets reset
        else:
            self.over_field = over_arg
        
    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        
        if self.over_pipe:
            one_rec_upstream = UpstreamSource()
            one_rec_upstream.add_item(record)
            self.over_pipe.set_sources([one_rec_upstream])

        else: # else its over:field, get field data        
            field_data = record.pop(self.over_field, None)
            if not field_data:
                return record
        
            if isinstance(field_data, list):
                self.upstream_source.set_list(field_data)
            else:
                self.upstream_source.set_list([field_data])

        out_recs = []
        pipe = self.subexp_stack[-1]

        # reset components that are being reused across records
        for op in self.subexp_ops:
            if isinstance(op, Pipe):
                op.reset()
        
        while True:
            rec = pipe.next()
            if rec == None:
                break
            out_recs.append(rec)
        record[self.over_field] = out_recs

        # result for parent
        for op in self.subexp_ops:
            get_subexp = getattr(op, "get_subexp_result", None)
            if get_subexp:
                name, value = get_subexp()
                if name:
                    record[name] = value

        return record

class SubExpressionOver(Pipe):
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.over_arg = ptok.get_arg(0)

    def get_over_arg(self):
        return self.over_arg

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        return record
