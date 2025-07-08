import os
from typing import Iterable
from djk.base import Source
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
    def create(cls, path, source_class_getter, parms: str = ""):
        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        source_queue = Queue()
        for file in files:
            source_class = source_class_getter(file, parms)
            if source_class:
                lazy_file = LazyFileLocal(file)
                source_queue.put(source_class(lazy_file))
            else:
                raise SyntaxError(f'No format for file:{file}')
            
        if source_queue.empty():
            return None
        
        return DirSource(source_queue)
