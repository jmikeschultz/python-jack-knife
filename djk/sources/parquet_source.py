# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pyarrow.parquet as pq
from typing import Optional
from djk.base import Source
from djk.sources.lazy_file import LazyFile

class ParquetSource(Source):
    is_format = True  # enables format-based routing

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.file = None
        self.batch = None
        self.row_index = 0
        self.num_recs = 0

    def next(self) -> Optional[dict]:
        # Lazy init
        if self.file is None:
            self.file = self.lazy_file.open(binary=True)
            table = pq.read_table(self.file)
            self.batch = table.to_pydict()
            self.num_rows = len(next(iter(self.batch.values()))) if self.batch else 0
            self.row_index = 0

        # Done reading
        if self.row_index >= self.num_rows:
            self.file.close()
            self.file = None
            return None

        # Build dict for the current row
        record = {
            col: self.batch[col][self.row_index]
            for col in self.batch
        }
        self.row_index += 1
        self.num_recs += 1
        return record
