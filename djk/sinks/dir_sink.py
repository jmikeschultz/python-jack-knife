import os
from djk.base import Source, Sink

class DirSink(Sink):
    def __init__(self, source: Source, path: str, sink_class: str, parms: str):
        self.source = source
        self.dir_path = path
        self.sink_class = sink_class
        self.parms = parms
        self.fileno = 0

    def process(self):
        fileSink = self.sink_class(self.source, os.path.join(self.dir_path, f"file-{self.fileno:04d}"))
        fileSink.process()
