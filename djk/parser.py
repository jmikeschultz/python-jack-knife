from typing import Any, List, Callable
import os

from djk.base import Source, Sink, Pipe, IdentitySource
from djk.pipes.factory import PipeFactory
from djk.pipes.common import add_operator
from djk.sources.factory import SourceFactory
from djk.sinks.sinks import JsonSink, StdoutYamlSink
from djk.sinks.devnull import DevNullSink
from djk.sinks.graph import GraphSink
from djk.sinks.csv import CSVSink
from djk.sinks.ddb import DDBSink

def expand_macros(tokens: List[str]) -> List[str]:
    expanded = []
    for token in tokens:
        if token.endswith(".pjk"):
            if not os.path.isfile(token):
                raise FileNotFoundError(f"Macro file not found: {token}")
            with open(token, "r") as f:
                lines = f.readlines()
            stripped = [
                line.split("#", 1)[0].strip()
                for line in lines
            ]
            joined = " ".join(stripped)
            expanded += joined.split()
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
            raise RuntimeError("This is bad, need syntax error")
        
        penult = self.stack.pop()
        if not isinstance(penult, Source):
            raise RuntimeError("Penultimate element is not a source")

        sink_factory = self._resolve_sink(sink_token)
        return sink_factory(penult)

    def _resolve_sink(self, token: str) -> Callable[[Source], Sink]:
        if token == "-":
            return StdoutYamlSink
        elif token.endswith(".json") or token.endswith(".json.gz"):
            return lambda src: JsonSink(src, token)
        elif token.endswith(".csv"):
            return lambda src: CSVSink(src, token)        
        elif token == "devnull":
            return lambda src: DevNullSink(src)
        elif token.startswith("graph:"):
            kind = token.split(":", 1)[1]
            return lambda src: GraphSink(src, kind)
        elif token.startswith('ddb:'):
            instance = token.split(":", 1)[1]
            return lambda src: DDBSink(src, instance)


        else:
            raise SyntaxError("Expression must end in a sink (e.g. '.', 'json:out.json')")
