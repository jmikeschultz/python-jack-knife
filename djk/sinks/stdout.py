import sys
import yaml
import subprocess
import shutil
from djk.base import Sink, Source, ParsedToken, Usage

class StdoutSink(Sink):
    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.use_pager = True # FIX ME
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
            
