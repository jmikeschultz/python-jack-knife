from typing import Any, List, Callable
import os
from djk.base import Source, Sink, ParsedToken, TokenError
from djk.sinks.sinks import StdoutYamlSink
from djk.sinks.json_sink import JsonSink, JsonGzSink
from djk.sinks.devnull import DevNullSink
from djk.sinks.graph import GraphSink
from djk.sinks.csv_sink import CSVSink
from djk.sinks.tsv_sink import TSVSink
from djk.sinks.ddb import DDBSink
from djk.sinks.dir_sink import DirSink
from djk.sinks.user_sink_factory import UserSinkFactory

class SinkFactory:
    file_formats = {'json': JsonSink,
                    'json.gz': JsonGzSink,
                    'csv': CSVSink,
                    'tsv': TSVSink}
    
    @classmethod
    def _resolve_file_sinks(cls, token: str):
        clazz = cls.file_formats.get(token, None)
        if clazz:
            return None, clazz

        # for file paths        
        for ext, sink_class in cls.file_formats.items():
            if token.endswith(f'.{ext}'):
                path_no_ext = token.removesuffix(f'.{ext}')
                return path_no_ext, sink_class

        return None, None
    
    @classmethod
    def _create_dir_sinks(cls, source, main, parms):
        parts = main.split(':')
        if len(parts) != 3:
            return None

        filesys, format, path = parts
        _, sink_class = cls._resolve_file_sinks(format)
        if not sink_class:
            raise UsageError(f'No such format:{format}')

        if 'dir' in filesys:
            os.makedirs(path, exist_ok=True)
            return DirSink(source, path, sink_class, parms)
        
#        if 's3' in filesys:
#            retu
        
        return None

    @classmethod
    def create(cls, token: str, source: Source) -> Callable[[Source], Sink]:
        token = token.strip()

        parts = token.split(',', 1) # the separator for optional params
        main = parts[0]
        parms = parts[1] if len(parts) > 1 else ""

        ptok = ParsedToken(token)

        if ptok.main.endswith('.py'):
            sink = UserSinkFactory.create(ptok, source)
            if sink:
                return sink

        if token == "-":
            return StdoutYamlSink(source, token)
        elif token == "devnull":
            return DevNullSink(source, token)
        elif token.startswith("graph:"):
            kind = token.split(":", 1)[1]
            return GraphSink(source, kind)
        elif token.startswith('ddb:'):
            table = token.split(":", 1)[1]
            return DDBSink(source, table)

        # filesystem:format:path e.g.
        # dir:json:path, s3:json:path
        sink = cls._create_dir_sinks(source, main, parms)
        if sink:
            return sink

        # when main is file path with format
        path_no_ext, sink_class = cls._resolve_file_sinks(main)
        if sink_class:
            return sink_class(source, path_no_ext)

        else:
            raise TokenError(token, 'pjk <source> [<pipe> ...] <sink>', ["Expression must end in a sink (e.g. '-', 'out.json')"])


