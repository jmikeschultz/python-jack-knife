from typing import Any, List, Callable
import os
import shlex
from djk.base import Source, Sink, Pipe, IdentitySource
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
                    raise SyntaxError(f"Error parsing {token}: {e}")
            expanded.extend(stripped)
        else:
            expanded.append(token)
    return expanded


class DjkExpressionParser:
    def __init__(self, tokens: List[str]):
        self.tokens = expand_macros(tokens)
        if not self.tokens:
            raise SyntaxError("Empty expression")
        if len(self.tokens) < 2:
            raise SyntaxError("Expression must end in a sink (e.g. '-', 'out.json')")
        self.stack: List[Any] = []

    def parse(self) -> Sink:
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
                raise ValueError(f"Unrecognized token: {token}")
        
        #
        if len(self.stack) != 1:
            raise SyntaxError("This is bad, need syntax error")
        
        penult = self.stack.pop()
        if not isinstance(penult, Source):
            raise SyntaxError("Penultimate component is not a source")

        sink = SinkFactory.create(sink_token, penult)
        return sink

    