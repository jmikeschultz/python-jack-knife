import os
import gzip
import json
from djk.base import Sink, Source, ParsedToken, Usage

class JsonSink(Sink):
    is_format = True

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.path_no_ext = ptok.main
        self.gz = ptok.get_arg(0) == 'True'

    def process(self) -> None:
        path = self.path_no_ext + ('.json.gz' if self.gz else '.json')
        open_func = gzip.open if self.gz else open

        with open_func(path, 'wt', encoding='utf-8') as f:
            while True:
                record = self.input.next()
                if record is None:
                    break
                f.write(json.dumps(record) + '\n')

