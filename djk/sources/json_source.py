# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from typing import Optional
from djk.base import Source
from djk.sources.lazy_file import LazyFile

class JsonSource(Source):
    is_format = True
    
    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.file = None
        self.num_recs = 0

    def next(self) -> Optional[dict]:
        if self.file is None:  # lazy init
            self.file = self.lazy_file.open()

        while True:
            line = self.file.readline()
            if not line:
                self.file.close()
                self.file = None
                return None
            else:
                self.num_recs += 1
                try:
                    return json.loads(line)
                except json.JSONDecodeError as e:
                    print(f'Skipping json line {self.num_recs} with format error: {line}')
                    continue

