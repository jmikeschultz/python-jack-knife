import json
import gzip
from typing import Optional
from djk.base import Source, Report

class JsonSource(Source):
    def __init__(self, path_or_stream):
        self.file = None
        self.num_recs = 0

        if isinstance(path_or_stream, str):
            self.path = path_or_stream
            self.file = None
        else:
            self.path = "<stream>"
            self.file = path_or_stream

    def lazy_init(self):
        if self.path.endswith('.gz'):
            self.file = gzip.open(self.path, 'rt')
        else:
            self.file = open(self.path, 'r')

    def next(self) -> Optional[dict]:
        if self.file is None: # lazy 
            self.lazy_init()

        line = self.file.readline()
        if not line:
            self.file.close()
            self.file = None
            return None
        else:
            self.num_recs += 1
            return json.loads(line)
