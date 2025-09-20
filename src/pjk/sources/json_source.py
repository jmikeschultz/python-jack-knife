# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from pjk.base import Source, NoBindUsage
from pjk.sources.lazy_file import LazyFile
from pjk.sources.format_source import FormatSource
from pjk.log import logger

class JsonSource(FormatSource):
    extension = 'json'

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_recs = 0

    def __iter__(self):
        with self.lazy_file.open() as f:
            for line in f:
                self.num_recs += 1
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    print('json decode error, see ~/.pjk/logs')
                    snippet = line.strip()
                    if len(snippet) > 200:
                        snippet = snippet[:200] + "…"
                    logger.warning(
                        f"Skipping invalid JSON at line {self.num_recs} "
                        f"in {self.lazy_file.path}: {e} | data: {snippet}"
                    )
