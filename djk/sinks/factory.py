from typing import Any, List, Callable
import os
from djk.base import Source, Sink, ParsedToken, TokenError, UsageError, ComponentFactory
from djk.sinks.stdout import StdoutSink
from djk.sinks.json_sink import JsonSink
from djk.sinks.devnull import DevNullSink
from djk.sinks.graph import GraphSink
from djk.sinks.csv_sink import CSVSink
from djk.sinks.tsv_sink import TSVSink
from djk.sinks.ddb import DDBSink
from djk.sinks.dir_sink import DirSink
from djk.sinks.expect import ExpectSink
from djk.sinks.user_sink_factory import UserSinkFactory

class SinkFactory(ComponentFactory):
    HEADER = 'SINKS'
    COMPONENTS = {
        '-': StdoutSink,
        'devnull': DevNullSink,
        'graph': GraphSink,
        'ddb': DDBSink,
        'json': JsonSink,
        'csv': CSVSink,
        'tsv': TSVSink,
        'expect': ExpectSink
        }

    @classmethod
    def create(cls, token: str, source: Source) -> Callable[[Source], Sink]:
        token = token.strip()
        ptok = ParsedToken(token)

        if ptok.main.endswith('.py'):
            sink = UserSinkFactory.create(ptok, source)
            if sink:
                return sink
        
        sink_cls = cls.COMPONENTS.get(ptok.main)
        if not sink_cls:
            # attempt case -> myfile.<format>
            return cls._attempt_format_file(source, ptok)
        
         # case -> <format>:<path> local dir
        if sink_cls.is_format: 
            dir_usage = DirSink.usage()
            dir_usage.bind(ptok)
            return DirSink(source, ptok, dir_usage, sink_cls)        

        usage = sink_cls.usage()
        usage.bind(ptok)

        sink = sink_cls(source, ptok, usage)
        if sink:
            return sink

        # when main is file path with format
        path_no_ext, sink_class = cls._resolve_file_sinks(ptok.main)
        if sink_class:
            return sink_class(source, path_no_ext)

        else:
            raise TokenError.from_list(['pjk <source> [<pipe> ...] <sink>',
                                        "Expression must end in a sink (e.g. '-', 'out.json')"]
                                        )

    @classmethod
    def _attempt_format_file(cls, source: Source, ptok: ParsedToken):
        is_gz = False
        path, ext = os.path.splitext(ptok.main)
        if '.gz' in ext:
            is_gz = True
            path, ext = os.path.splitext(path)
        
        file_ext = ext.lstrip('.')  # removes the leading dot

        sink_cls = cls.COMPONENTS.get(file_ext)
        if not sink_cls:
            return None
        
        file_token = f'{path}:{is_gz}' # hack so user can do .json.gz
        file_ptok = ParsedToken(file_token)
        
        usage = sink_cls.usage()
        usage.bind(file_ptok) # not sure we'll ever use since we're hacking above

        return sink_cls(source, file_ptok, usage)
        

