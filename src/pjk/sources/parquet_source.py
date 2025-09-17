# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Source
from pjk.sources.lazy_file import LazyFile
from pjk.sources.format_usage import FormatUsage

class ParquetSource(Source):
    is_format = True  # enables format-based routing
    @classmethod
    def usage(cls):
        return FormatUsage('parquet', component_class=cls)

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_recs = 0

    def __iter__(self):
        import pyarrow.parquet as pq # lazy import
        with self.lazy_file.open(binary=True) as f:
            table = pq.read_table(f)
            batch = table.to_pydict()

            if not batch:
                return  # no columns = no rows

            num_rows = len(next(iter(batch.values())))

            for i in range(num_rows):
                record = {col: batch[col][i] for col in batch}
                self.num_recs += 1
                yield record
