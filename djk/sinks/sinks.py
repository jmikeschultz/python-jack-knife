import json
import sys
import yaml
import gzip
import subprocess
import shutil
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

class YamlSink(Sink):
    def __init__(self, input_source: Source, path: str):
        super().__init__(input_source)
        self.path = path

    def process(self) -> None:
        with open(self.path, 'w') as f:
            docs = []
            while True:
                record = self.input.next()
                if record is None:
                    break
                docs.append(record)
            yaml.dump_all(docs, f, sort_keys=False)


class StdoutYamlSink(Sink):
    def __init__(self, input_source: Source, use_pager: bool = True):
        super().__init__(input_source)
        self.use_pager = use_pager
        self.suppress_report = True

    def process(self) -> None:
        output_stream = sys.stdout
        pager_proc = None

        if self.use_pager and shutil.which("less"):
            pager_proc = subprocess.Popen(["less", "-FRSX"], stdin=subprocess.PIPE, text=True)
            output_stream = pager_proc.stdin

        try:
            while True:
                record = self.input.next()
                if record is None:
                    break
                try:
                    yaml.dump(record, output_stream, sort_keys=False, explicit_start=True, width=float("inf"))
                except BrokenPipeError:
                    break  # user quit pager
        except BrokenPipeError:
            pass  # outer fallback
        finally:
            if pager_proc:
                try:
                    output_stream.close()
                except BrokenPipeError:
                    pass
                pager_proc.wait()
            
