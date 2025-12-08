# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
from pjk.usage import NoBindUsage
from pjk.components import Source
from pjk.sources.format_source import FormatSource
from pjk.sources.lazy_file import LazyFile


class SQLSource(FormatSource):
    extension = 'sql'
    desc_override = "SQL source. Emits SQL in single record in 'query' field."

    def __init__(self, lazy_file: LazyFile):
        super().__init__(root=None)
        self.lazy_file = lazy_file

    def __iter__(self):
        lines = []
        with self.lazy_file.open() as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue

                if '#' in line:
                    line = line.split('#')[0]
                if '--' in line:
                    line = line.split('--')[0]
                lines.append(line)

            sql_text = ' '.join(lines)

            if sql_text:
                yield {"query": sql_text}
