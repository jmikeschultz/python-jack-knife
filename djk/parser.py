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
        try:
            copies = self.tokens[:-1] # exclude sink
            sink_token = self.tokens[-1] # sink

            for token in copies:
                source = SourceFactory.create(token)
                if source:
                    add_operator(source, self.stack)
                    continue

                pipe = PipeFactory.create(token)
                if pipe:
                    add_operator(pipe, self.stack)
                    continue

                else:
                    raise UsageError(f"Unrecognized token: {token}")
            
            #
            if len(self.stack) != 1:
                raise UsageError("This is bad, need syntax error")
            
            penult = self.stack.pop()
            if not isinstance(penult, Source):
                raise UsageError("Penultimate component is not a source")

            sink = SinkFactory.create(sink_token, penult)
            return sink
        
        except TokenError as e:
            #print(e, file=sys.stderr)
            #sys.exit(2)  # Exit with a non-zero code, but no traceback
            raise UsageError('message goes here', e)
            
        

    