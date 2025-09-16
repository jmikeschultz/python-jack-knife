# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from typing import Any
from queue import Queue, Empty
from pjk.base import Source, ParsedToken
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
    def create(cls, ptok: ParsedToken, get_format_class_gz: Any):
        params = ptok.get_params()
        override = params.get('format', None)
        path = ptok.all_but_params

        files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if os.path.isfile(os.path.join(path, f))
        ]

        source_queue = Queue()
        for file in files:
            file_token = file if not override else f"{file}@format={override}"
            file_ptok = ParsedToken(file_token)

            format_class, is_gz = get_format_class_gz(file_ptok)
            if format_class:
                lazy_file = LazyFileLocal(file, is_gz)
                source_queue.put(format_class(lazy_file))
            else:
                raise RuntimeError(f"No format for file: {file}")

        if source_queue.empty():
            return None

        return DirSource(source_queue)
