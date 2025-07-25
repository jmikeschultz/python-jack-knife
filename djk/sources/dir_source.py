# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from typing import Any
from djk.base import Source, ParsedToken
from queue import Queue, Empty
from djk.sources.lazy_file_local import LazyFileLocal
from djk.log import logger

class DirSource(Source):
    def __init__(self, source_queue: Queue, in_source: Source = None):
        self.source_queue = source_queue
        self.current = in_source

    def next(self):
        while True:
            if self.current is None:
                try:
                    self.current = self.source_queue.get_nowait()
                    logger.debug(f'next source={self.current}')
                except Empty:
                    return None

            record = self.current.next()
            if record is not None:
                return record
            else:
                self.current = None

    def deep_copy(self):
        if self.source_queue.qsize() <= 1:
            return None # leave for original DirSource
        try:
            next_source = self.source_queue.get_nowait()
            logger.debug(f'deep_copy next_source={next_source}')
        except Empty:
            return None

        return DirSource(self.source_queue, next_source)

    @classmethod
    def create(cls, ptok: ParsedToken, get_format_class_gz: Any):
        params = ptok.get_params() # ptok is for the directory, not the files
        override = params.get('format', None)

        path = ptok.all_but_params
        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        source_queue = Queue()
        for file in files:

            # build a ptok for each file
            file_token = f'{file}' + '' if not override else f'format={override}'
            file_ptok = ParsedToken(file_token)

            format_class, is_gz = get_format_class_gz(file_ptok)
            if format_class:
                lazy_file = LazyFileLocal(file, is_gz)
                source_queue.put(format_class(lazy_file))
            else:
                raise(f'Fix me in Dir Source: No format for file:{file}')
            
        if source_queue.empty():
            return None
        
        return DirSource(source_queue)
