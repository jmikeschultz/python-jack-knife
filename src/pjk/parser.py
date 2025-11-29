# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import shlex
from typing import Any, List
from pjk.components import Source, Pipe, Sink
from pjk.usage import TokenError, UsageError, ParsedToken, Usage
from pjk.pipes.let_reduce import ReducePipe
from pjk.pipes.progress_pipe import ProgressPipe
from pjk.registry import ComponentRegistry
from pjk.progress import papi
from typing import Dict
from pathlib import Path
from pjk.progress import ProgressIgnore

MACROS_FILE = '~/.pjk/macros.txt'
MACRO_PREFIX = 'm'
def read_macros(file_name: str = MACROS_FILE) -> Dict[str, str]:
    out: Dict[str, str] = {}
    path = Path(file_name).expanduser()
    with path.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, val = line.split(":", 1)
            out[key.strip()] = val.strip()
    return out

# macros are of the form MACRO_PREFIX:<instance>
def handle_macros(token: str, expanded: List[str]):
    if not token.startswith(f'{MACRO_PREFIX}:'):
        return False
    
    instance = token.split(':', 1)[1]
    macros = read_macros()
    fav = macros.get(instance, None)
    if not fav:
        raise TokenError(f"No '{instance}' macro in {MACROS_FILE}")

    try:
        parts = shlex.split(fav, comments=True, posix=True)
        expanded.extend(parts)
    except ValueError as e:
        raise UsageError(f"Error parsing {token}: {e}")
    
    return True

def handle_pjk_file(token: str, expanded: List[str]):
    if not token.endswith(".pjk"):
        return False
    
    if not os.path.isfile(token):
        raise TokenError(f"pjk file not found: {token}")
    
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
    return True

def expand_macros(tokens: List[str]) -> List[str]:
    expanded = []
    for token in tokens:
        if handle_macros(token, expanded):
            continue

        elif handle_pjk_file(token, expanded):
            continue
            
        else:
            expanded.append(token)

    return expanded

stack_level = -1
class OperandStack:
    def __init__(self):
        self.stack: List[Any] = []

    def push(self, op):
        global stack_level
        stack_level+=1
        papi.register_component(op, stack_level)
        self.stack.append(op)

    def pop(self):
        global stack_level
        stack_level-=1
        return self.stack.pop()
    
    def peek(self):
        if not len(self.stack):
            return None
        return self.stack[-1]
    
    def clear(self):
        self.stack.clear()
    
    def empty(self):
        return len(self.stack) == 0
    
    def print(self, toadd):
        print('---------')
        if toadd:
            print(f'{type(toadd).__name__}={id(toadd)}')
        if len(self.stack) == 0:
            print(f'Stack={id(self)} StackEmpty')
        for op in self.stack:
            print(f'Stack={id(self)} {type(op).__name__}={id(op)}')

class ExpressionParser:
    def __init__(self, registry: ComponentRegistry):
        self.stack = OperandStack()
        self.registry = registry

    def get_sink(self, stack_helper, token):
        if self.stack.empty():
            raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

        source = self.stack.pop()
        if isinstance(source, SubExpression):
            raise TokenError("Poorly formed sub-expression.  Begin token '[' without matching 'over' keyword." )

        if not self.stack.empty():
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
        
        # so each sink doesn't have to, maybe make a base class or mixin for sinks
        progress_pipe = ProgressPipe(component=sink)
        progress_pipe.add_source(source)

        sink.add_source(progress_pipe)
        return sink

    def parse(self, tokens: List[str]) -> Sink:
        usage_error_message = "You've got a problem here."
        stack_helper = StackLoader()
        self.tokens = tokens
        pos = 0

        try:
            self.tokens = expand_macros(tokens)

            if len(self.tokens) < 2:
                raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

            for pos, token in enumerate(self.tokens):
                if pos == len(self.tokens) - 1: # token should be THE sink
                    return self.get_sink(stack_helper, token)
                    
                source = self.registry.create_source(token)
                if source:                    
                    stack_helper.add_operator(source, self.stack)
                    progress_pipe = ProgressPipe(component=source, simple=True)
                    stack_helper.add_operator(progress_pipe, self.stack)
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
        super().__init__(None, None)
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

    def add_operator(self, op, stack: OperandStack):
        #stack.print(op)

        if not stack.empty() and isinstance(stack.peek(), SubExpression):
            subexp = stack.peek()

            if isinstance(op, SubExpressionOver) and subexp.recursion_depth() == 0:
                subexp = stack.pop()
                op.add_source(subexp)
                stack.push(op)

                global stack_level
                stack_level -=1 # not sure why this can't be handled exclusively by the stack
                return
            
            else: # an operator within the subexpression
                subexp = stack.peek()
                subexp.add_subop(op)
                return

        if isinstance(op, SubExpressionOver):
            if stack.empty or not isinstance(stack.peek(), SubExpression):
                raise TokenError("Poorly formed sub-expression.  'over' keyword without matching begin token '['.")
            op.add_source(stack.pop())
            stack.push(op)

        # order matters, because sources are pipes
        if isinstance(op, Pipe):
            arity = op.arity # class level attribute
            for _ in range(arity):
                if stack.empty():
                    name = type(op).usage().name
                    raise TokenError(f"'{name}' requires {arity} input(s)")
                op.add_source(stack.pop())
            stack.push(op)

            if isinstance(op, ReducePipe):
                self.top_level_reducers.append(op)

            return

        elif isinstance(op, Source):
            stack.push(op)
            return
            
# special upstream source put in subexp stack for flexibility
# when we don't know what that upstream source will be.
class UpstreamSource(Source):
    # used only by progress
    @classmethod
    def usage(cls) -> Usage:
        u = Usage(
            name="[",
            desc="sub-expression begin.",
            component_class=cls,
        )
        return u
    
    def __init__(self):
        super().__init__(root=None)
        self.data = []
        self.inner_source = None
        self.sub_recs_in = papi.get_counter(self, var_label='sub_recs_in')

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
            for rec in self.inner_source:
                self.sub_recs_in.increment()
                yield rec
        else:
            for item in self.data:
                self.sub_recs_in.increment()
                yield item
    
class SubExpressionOver(Pipe):
    @classmethod
    def usage(cls) -> Usage:
        u = Usage(
            name="over",
            desc="sub-expression over.",
            component_class=cls,
        )
        return u
    
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.over_arg = ptok.get_arg(0)

    def reset(self):
        pass  # stateless

    def __iter__(self):
        if not isinstance(self.left, SubExpression):
            raise Exception('this actually cannot happen, but did')

        for record in self.left:
            self.left.subexp_process(record, self.over_arg)
            yield record

class SubExpression(Pipe, ProgressIgnore):
    @classmethod
    def create(cls, token: str) -> Pipe:
        ptok = ParsedToken(token)
        if ptok.pre_colon == '[':
            return SubExpression(ptok, None)
        if ptok.pre_colon == 'over':
            return SubExpressionOver(ptok, None)
        return None

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.subexp_ops = []
        self.stack_helper = StackLoader()
        self.subexp_stack = OperandStack() 
        self.upstream_source = UpstreamSource()
        self.subexp_stack.push(self.upstream_source)
        self.recursions = 0 # number of subexpression within
        self.subexp_left = None

    def add_subop(self, op):
        self.subexp_ops.append(op)
        if isinstance(op, SubExpression):
            self.recursions += 1
        elif isinstance(op, SubExpressionOver):
            self.recursions -= 1
        self.stack_helper.add_operator(op, self.subexp_stack)

    def recursion_depth(self):
        return self.recursions
    
    def reset(self):
        for op in self.subexp_ops:
            if isinstance(op, Pipe):
                op.reset()

    def __iter__(self):
        yield from self.left # pass thru to subexp_over which then calls process

    def set_upstream(self, over_arg: str, record: dict):
        if over_arg == '+':
            self.upstream_source.set_list([record])
            return True

        field_data = record.pop(over_arg, None)
        if not field_data:
            return False
        
        if isinstance(field_data, list):
            self.upstream_source.set_list(field_data)
        else:
            self.upstream_source.set_list([field_data])

        return True

    def subexp_process(self, record: dict, over_arg: str):
        if not self.subexp_left:
            self.subexp_left = self.subexp_stack.pop()

        if not self.set_upstream(over_arg, record):
            return # couldn't set, no processing

        # Reset sub-pipe stack
        for op in self.subexp_ops:
            op.reset()

        out_recs = []

        for rec in self.subexp_left:
            out_recs.append(rec)

        field = over_arg if over_arg != '+' else 'child'
        record[field] = out_recs

        for op in self.subexp_ops:
            if isinstance(op, ReducePipe):
                name, value = op.get_subexp_result()
                if name:
                    record[name] = value
