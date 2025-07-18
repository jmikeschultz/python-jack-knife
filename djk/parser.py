from typing import Any, List, Callable
import os
import sys
import shlex
from djk.base import Source, Sink, TokenError, UsageError
from djk.pipes.factory import PipeFactory
from djk.sinks.factory import SinkFactory
from djk.pipes.common import add_operator
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
    usage_error_message = "You've got a problem here."
    
    def __init__(self, tokens: List[str]):
        self.tokens = expand_macros(tokens)
        self.stack: List[Any] = []

    def parse(self) -> Sink:
        usage_error_message = "You've got a problem here."
        pos = 0
        try:
            if len(self.tokens) < 2:
                raise TokenError.from_list(['expression must include source and sink.',
                                            'pjk <source> [<pipe> ...] <sink>'])

            for pos, token in enumerate(self.tokens):
                if pos == len(self.tokens) - 1: # should be sink
                    if len(self.stack) > 0:
                        penult = self.stack.pop()
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
                    add_operator(source, self.stack)
                    continue

                pipe = PipeFactory.create(token)
                if pipe:
                    add_operator(pipe, self.stack)
                    continue

                else: # unrecognized token
                    raise TokenError.from_list([token, 'unrecognized token'])
        
        except TokenError as e:
            raise UsageError(usage_error_message, self.tokens, pos, e)
        
        
        
            
        

    