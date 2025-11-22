# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from pjk.usage import NoBindUsage
from pjk.components import Source
from pjk.sources.lazy_file import LazyFile
from pjk.sources.format_source import FormatSource
from typing import Any, Dict, Iterable, Optional
from pjk.log import logger

class JsonSource(FormatSource):
    extension = 'json'

    def __init__(self, lazy_file: LazyFile):
        super().__init__(root=None)
        self.lazy_file = lazy_file
        self.num_recs = 0

    def as_whole_file(self) -> Iterable[Dict[str, Any]]:
        with self.lazy_file.open() as f:
            string = f.read()
            object = json.loads(string)
            if isinstance(object, list):
                for item in object:
                    yield item
            else: 
                yield object

    def __iter__(self):
        with self.lazy_file.open() as f:
            for line in f:
                self.num_recs += 1
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    try:
                        yield from self.as_whole_file()
                        return
                    except:
                        logger.error(f'cannot decode {self.lazy_file.path}')
                        break
