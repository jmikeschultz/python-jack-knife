# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import csv
from djk.base import Sink, Source, ParsedToken, Usage

class CSVSink(Sink):
    is_format = True

    @classmethod
    def usage(cls):
        usage = Usage(
            name='csv',
            desc='Write records to a CSV file with dynamic header from first record'
        )
        usage.def_arg('path', usage='Path prefix (no extension)')
        usage.def_param('delim', usage='CSV delimiter (default: ",")', default=',')
        usage.def_param('ext', usage='File extension (default: csv)', default='csv')
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        path_no_ext = usage.get_arg('path')
        self.delimiter = usage.get_param('delim', default=',')
        ext = usage.get_param('ext', default='csv')
        self.path = f"{path_no_ext}.{ext}"

    def process(self) -> None:
        with open(self.path, 'w', newline='') as f:
            writer = None

            for record in self.input:
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=record.keys(), delimiter=self.delimiter)
                    writer.writeheader()
                writer.writerow(record)
