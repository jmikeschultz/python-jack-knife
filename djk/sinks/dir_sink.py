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
            desc='Write records to a local directory in the given <format> (e.g., csv)',
            component_class=cls
        )
        usage.def_arg(name='dir', usage='Path to output directory')
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage, sink_class: type, fileno: int = 0):
        super().__init__(ptok, usage)
        self.dir_path = usage.get_arg('dir')  # ✅ Use usage, not ptok directly

        self.ptok = ptok
        self.usage = usage
        self.sink_class = sink_class
        self.fileno = fileno
        self.num_files = 1

        os.makedirs(self.dir_path, exist_ok=True)

    def process(self):
        file = os.path.join(self.dir_path, f'file-{self.fileno:04d}')
        file_ptok = ParsedToken(file)
        file_usage = self.sink_class.usage()
        file_usage.bind(file_ptok)

        file_sink = self.sink_class(file_ptok, file_usage)
        file_sink.add_source(self.input)

        logger.debug(f'in process sinking to: {file}')
        file_sink.process()

    def deep_copy(self):
        source_clone = self.input.deep_copy()
        if not source_clone:
            return None

        clone = DirSink(
            source=source_clone,
            ptok=self.ptok,
            usage=self.usage,
            sink_class=self.sink_class,
            fileno=self.num_files
        )
        self.num_files += 1
        return clone
