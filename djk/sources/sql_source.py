# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
from djk.base import Source, NoBindUsage
from djk.sources.format_usage import FormatUsage
from djk.sources.lazy_file import LazyFile


class SQLSource(Source):
    is_format = True

    @classmethod
    def usage(cls):
        return FormatUsage(
            "sql",
            component_class=cls,
            desc_override="SQL source. Emits SQL in single record in 'query' field."
        )

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_recs = 0

    def __iter__(self):
        with self.lazy_file.open() as f:
            sql_text = f.read().strip()
            sql_text = sql_text.replace("\r", " ").replace("\n", " ").strip()

            if sql_text:
                self.num_recs += 1
                yield {"query": sql_text}
