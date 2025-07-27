# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.sinks.csv_sink import CSVSink
from djk.base import Source, ParsedToken, Usage

class TSVSink(CSVSink):
    is_format = True

    @classmethod
    def usage(cls):
        usage = Usage(
            name='tsv',
            desc='Write records to a .tsv file (tab-separated values)'
        )
        usage.def_arg(name='path', usage='Path prefix (no extension)')
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        path_no_ext = usage.get_arg('path')
        super().__init__(input_source, path_no_ext, delimiter="\t", ext='tsv')
