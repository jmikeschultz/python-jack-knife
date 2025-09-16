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
            desc='Write records to a CSV file with dynamic header from first record',
            component_class=cls
        )
        usage.def_arg('path', usage='Path prefix (no extension)')
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        path_no_ext = usage.get_arg('path')
        self.path = f"{path_no_ext}.csv"

    def process(self) -> None:
        with open(self.path, 'w', newline='') as f:
            writer = None

            for record in self.input:
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=record.keys())
                    writer.writeheader()
                writer.writerow(record)
