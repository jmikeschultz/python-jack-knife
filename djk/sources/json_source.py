# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from djk.base import Source
from djk.sources.lazy_file import LazyFile

class JsonSource(Source):
    is_format = True

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_recs = 0

    def __iter__(self):
        with self.lazy_file.open() as f:
            for line in f:
                self.num_recs += 1
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    print(f'Skipping json line {self.num_recs} with format error: {line.strip()}')
