import csv
from typing import Optional
from djk.base import Source
from djk.sources.lazy_file import LazyFile

class CSVSource(Source):
    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.file = None
        self.reader = None
        self.num_recs = 0

    def next(self) -> Optional[dict]:
        if self.reader is None:
            self.file = self.lazy_file.open()
            self.reader = csv.DictReader(self.file)

        try:
            row = next(self.reader)
            self.num_recs += 1
            return row
        except StopIteration:
            if self.file:
                self.file.close()
                self.file = None
            return None
