# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from djk.base import Source, NoBindUsage
from djk.sources.lazy_file import LazyFile
from djk.sources.format_usage import FormatUsage


class JsonSource(Source):
    is_format = True
    @classmethod
    def usage(cls):
        return FormatUsage('json', 'json lines source for local files and directories and also s3', component_class=cls)

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
