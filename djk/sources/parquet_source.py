# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pyarrow.parquet as pq
from djk.base import Source, NoBindUsage
from djk.sources.lazy_file import LazyFile
from djk.sources.format_usage import FormatUsage

class ParquetSource(Source):
    is_format = True  # enables format-based routing
    @classmethod
    def usage(cls):
        return FormatUsage('parquet', 'parquet source for s3 and local files and directories.', component_class=cls)

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_recs = 0

    def __iter__(self):
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
