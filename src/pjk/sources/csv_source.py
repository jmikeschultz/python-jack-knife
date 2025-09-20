# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import csv
from pjk.base import Source, NoBindUsage
from pjk.sources.format_source import FormatSource
from pjk.sources.lazy_file import LazyFile

csv.field_size_limit(sys.maxsize)

class CSVSource(FormatSource):
    extension = 'csv'

    def __init__(self, lazy_file: LazyFile, delimiter: str = ","):
        self.lazy_file = lazy_file
        self.delimiter = delimiter
        self.num_recs = 0

    def __iter__(self):
        with self.lazy_file.open() as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                self.num_recs += 1
                yield row
