# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from djk.base import Source, Sink, ParsedToken, Usage
from djk.log import logger

class DirSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='<format>',
            desc='write records to a local directory in <format>'
        )
        usage.def_arg(name='dir', usage='path to directory')
        return usage

    def __init__(self, source: Source, ptok: ParsedToken, usage: Usage, sink_class: str, fileno: int = 0):
        self.source = source
        self.ptok = ptok
        self.usage = usage

        # till we get usage working
        self.dir_path = ptok.get_arg(0)

        self.sink_class = sink_class
        self.fileno = fileno # root node gets fileno = 0
        self.num_files = 1

        os.makedirs(self.dir_path, exist_ok=True)

    def process(self):
        # build a ptok for the format sink
        file = os.path.join(self.dir_path, f'file-{self.fileno:04d}')
        file_ptok = ParsedToken(file)
        file_usage = self.sink_class.usage()

        file_usage.bind(file_ptok)

        fileSink = self.sink_class(self.source, file_ptok, file_usage)
        logger.debug(f'in process sinking to: {file}')
        fileSink.process()

    def deep_copy(self):
        source_clone = self.source.deep_copy()
        if not source_clone:
            return None
        
        clone = DirSink(source_clone, self.ptok, self.usage, self.sink_class, self.num_files)
        self.num_files += 1
        return clone
        
