# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from typing import Any
from queue import Queue, Empty
from pjk.base import Source
from pjk.sources.lazy_file_local import LazyFileLocal
from pjk.log import logger

class DirSource(Source):
    def __init__(self, source_queue: Queue, in_source: Source = None):
        self.source_queue = source_queue
        self.current = in_source

    def __iter__(self):
        while True:
            if self.current is None:
                try:
                    self.current = self.source_queue.get_nowait()
                    logger.debug(f'next source={self.current}')
                except Empty:
                    return  # end of all sources

            try:
                for record in self.current:
                    yield record
            finally:
                self.current = None  # move to next source after exhaustion

    def deep_copy(self):
        if self.source_queue.qsize() <= 1:
            return None  # leave remaining files to original
        try:
            next_source = self.source_queue.get_nowait()
            logger.debug(f'deep_copy next_source={next_source}')
        except Empty:
            return None

        return DirSource(self.source_queue, next_source)
    
    @classmethod
    def get_format_gz(cls, input:str):
        is_gz = False
        format = input
        if input.endswith('.gz'):
            is_gz = True
            format = input[:-3]
        return format, is_gz

    @classmethod
    def create(cls, sources: dict, path_no_ext: str, format_override: str = None):
        files = [
            os.path.join(path_no_ext, f)
            for f in os.listdir(path_no_ext)
            if os.path.isfile(os.path.join(path_no_ext, f))
        ]

        source_queue = Queue()
        for file in files:
            parts = file.split('.')
            is_gz = False

            if parts[-1] == 'gz':
                is_gz = True
                parts.pop()

            format = parts[-1]
            
            if format_override:
                format, is_gz = cls.get_format_gz(format_override)

            source_class = sources.get(format)
            lazy_file = LazyFileLocal(file, is_gz)
            source_queue.put(source_class(lazy_file))

        if source_queue.empty():
            return None

        return DirSource(source_queue)
