from typing import Iterable
from djk.base import Source

class SourceListSource(Source):
    def __init__(self, source_iter: Iterable[Source]):
        self.sources = source_iter
        self.current = None

    def next(self):
        while True:
            if self.current is None:
                try:
                    self.current = next(self.sources)
                except StopIteration:
                    return None

            record = self.current.next()
            if record is not None:
                return record
            else:
                self.current = None
