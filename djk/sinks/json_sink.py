import gzip
import json
from djk.base import Sink, Source

class JsonSink(Sink):
    def __init__(self, input_source: Source, path: str):
        super().__init__(input_source)
        self.path = path

    def process(self) -> None:
        open_func = gzip.open if self.path.endswith('.gz') else open
        
        with open_func(self.path, 'wt') as f:
            while True:
                record = self.input.next()
                if record is None:
                    break
                f.write(json.dumps(record) + '\n')
