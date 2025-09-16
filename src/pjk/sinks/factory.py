# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Any, List, Callable
import os
from pjk.base import Source, Sink, ParsedToken
from pjk.common import ComponentFactory
from pjk.sinks.stdout import StdoutSink
from pjk.sinks.json_sink import JsonSink
from pjk.sinks.devnull import DevNullSink
from pjk.sinks.graph import GraphSink
from pjk.sinks.csv_sink import CSVSink
from pjk.sinks.tsv_sink import TSVSink
from pjk.sinks.ddb import DDBSink
from pjk.sinks.dir_sink import DirSink
from pjk.sinks.expect import ExpectSink
from pjk.sinks.user_sink_factory import UserSinkFactory

COMPONENTS = {
        '-': StdoutSink,
        'devnull': DevNullSink,
        'graph': GraphSink,
        'ddb': DDBSink,
        'json': JsonSink,
        'csv': CSVSink,
        'tsv': TSVSink,
        }

class SinkFactory(ComponentFactory):
    def __init__(self):
        super().__init__(COMPONENTS, 'sink')   

    def create(self, token: str) -> Callable[[Source], Sink]:
        token = token.strip()
        ptok = ParsedToken(token)

        # non-usage sink (bind incompatible)
        if ptok.pre_colon == 'expect':
            return ExpectSink(ptok, None)

        if ptok.pre_colon.endswith('.py'):
            sink = UserSinkFactory.create(ptok)
            if sink:
                return sink
        
        #if ptok.all_but_params.startswith('s3'):
        #    return S3Sink.create(ptok, get_format_class_gz=self.get_format_class_gz)

        # check for format sinks
        sink = self._attempt_format(ptok)
        if sink:
            return sink

        sink_cls = self.components.get(ptok.pre_colon)
        if not sink_cls:
            return None
        
        usage = sink_cls.usage()
        usage.bind(ptok)
        return sink_cls(ptok, usage)

        #raise TokenError.from_list(['pjk <source> [<pipe> ...] <sink>',
        #                            "Expression must end in a sink (e.g. '-', 'out.json')"]
        #                            )
        
    def _attempt_format(self, ptok: ParsedToken):
        format = ptok.pre_colon
        is_gz = False
        if format.endswith('.gz'):
            format = format[:-3]
            is_gz = True

        sink_cls = self.components.get(format) # <format>: directory case
        if not sink_cls:
            # attempt case -> myfile.<format>
            return self._attempt_format_file(ptok)
        
        # case -> <format>:<path> local dir
        if sink_cls.is_format: 
            dir_usage = DirSink.usage()
            dir_usage.bind(ptok)
            return DirSink(ptok, dir_usage, sink_cls, is_gz)

        return None


    def _attempt_format_file(self, ptok: ParsedToken):
        is_gz = False
        path, ext = os.path.splitext(ptok.all_but_params)
        if '.gz' in ext:
            is_gz = True
            path, ext = os.path.splitext(path)
        
        file_ext = ext.lstrip('.')  # removes the leading dot

        sink_cls = self.components.get(file_ext)
        if not sink_cls:
            return None
        
        file_token = f'{path}:{is_gz}' # hack so user can do .json.gz
        file_ptok = ParsedToken(file_token)
        
        usage = sink_cls.usage()
        usage.bind(file_ptok) # not sure we'll ever use since we're hacking above

        return sink_cls(file_ptok, usage)
        

