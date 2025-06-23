from typing import Optional
from djk.base import Source, Report

class SourceListSource(Source):
    def __init__(self):
        self.sources = None
        self.curr_source = None

    def set_sources(self, items):
        self.index = 0
        self.sources = items if items else []
    
    def next(self) -> Optional[dict]:
        if self.curr_source is None:
            if len(self.sources) == 0:
                return None
            self.curr_source = self.sources.pop()

        rec = self.curr_source.next()
        if not rec:
            self.curr_source = None
            return self.next()

        return rec
    