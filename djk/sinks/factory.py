from typing import Any, List, Callable
import os
from djk.base import Source, Pipe, Sink, PipeSyntaxError
from djk.sources.csv_source import CSVSource
from djk.sinks.sinks import JsonSink, StdoutYamlSink
from djk.sinks.devnull import DevNullSink
from djk.sinks.graph import GraphSink
from djk.sinks.csv import CSVSink
from djk.sinks.ddb import DDBSink

from djk.sources.lazy_file import LazyFile
from djk.sources.lazy_file_local import LazyFileLocal

class SinkFactory:

    @classmethod
    def create(cls, token: str, source: Source) -> Callable[[Source], Sink]:
        token = token.strip()

        parts = token.split(',', 1) # the separator for optional params
        token = parts[0]
        parms = parts[1] if len(parts) > 1 else ""

        # idea for local/s3 allowing multithreaded
        # filesystem:format:path e.g.
        # dir:json:path, s3:json:path

        if token == "-":
            return StdoutYamlSink(source, token)
        elif token.endswith(".json") or token.endswith(".json.gz"):
            return JsonSink(source, token)
        elif token.endswith(".csv"):
            return CSVSink(source, token)        
        elif token == "devnull":
            return DevNullSink(source, token)
        elif token.startswith("graph:"):
            kind = token.split(":", 1)[1]
            return GraphSink(source, kind)
        elif token.startswith('ddb:'):
            table = token.split(":", 1)[1]
            return DDBSink(source, table)

        else:
            raise SyntaxError("Expression must end in a sink (e.g. '-', 'out.json')")

