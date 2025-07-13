import gzip
import json
from djk.base import Sink, Source

class JsonSink(Sink):
    def __init__(self, input_source: Source, path_no_ext: str, gz: bool = False):
        super().__init__(input_source)
        self.path_no_ext = path_no_ext
        self.gz = gz

    def process(self) -> None:
        path = self.path_no_ext + ('.json.gz' if self.gz else '.json')
        open_func = gzip.open if self.gz else open

        with open_func(path, 'wt', encoding='utf-8') as f:
            while True:
                record = self.input.next()
                if record is None:
                    break
                f.write(json.dumps(record) + '\n')

class JsonGzSink(JsonSink):
    def __init__(self, input_source: Source, path_no_ext: str):
        super().__init__(input_source, path_no_ext, gz=True)
