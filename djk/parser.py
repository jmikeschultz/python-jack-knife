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
    def __init__(self, tokens: List[str]):
        self.tokens = expand_macros(tokens)
        if not self.tokens:
            raise UsageError("Empty expression")
        if len(self.tokens) < 2:
            raise UsageError("Expression must end in a sink (e.g. '-', 'out.json')")
        self.stack: List[Any] = []

    def parse(self) -> Sink:
        copies = self.tokens[:-1] # exclude sink
        sink_token = self.tokens[-1] # sink
        usage_error_message = "You've got a problem here."

        for pos, token in enumerate(copies):
            try:
                source = SourceFactory.create(token)
                if source:
                    add_operator(source, self.stack)
                    continue

                pipe = PipeFactory.create(token)
                if pipe:
                    add_operator(pipe, self.stack)
                    continue

                else: # unrecognized token
                    raise TokenError(token, None, ['unrecognized token'])
            
            except TokenError as e:
                raise UsageError(usage_error_message, self.tokens, pos, e)
            
        if len(self.stack) != 1:
            te = TokenError(sink_token, None, ['A sink can only consume one source.'])
            pos = len(self.tokens) - 1
            raise UsageError(usage_error_message, self.tokens, pos, te)
            
        penult = self.stack.pop()
        if not isinstance(penult, Source):
            te = TokenError(token, None, ['Penultimate component must be a source of records'])
            raise UsageError(usage_error_message, self.tokens, len(self.tokens)-1, te)

        try:
            sink = SinkFactory.create(sink_token, penult)
            return sink
        except TokenError as e:
            raise UsageError(usage_error_message, self.tokens, len(self.tokens)-1, e)
        
        
        
            
        

    