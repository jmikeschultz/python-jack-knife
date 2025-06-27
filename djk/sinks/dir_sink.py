import os
from djk.base import Source, Sink

class DirSink(Sink):
    def __init__(self, source: Source, path: str, sink_class: str, parms: str, fileno: int = 0):
        self.source = source
        self.dir_path = path
        self.sink_class = sink_class
        self.parms = parms
        self.fileno = fileno

    def process(self):
        fileSink = self.sink_class(self.source, os.path.join(self.dir_path, f'file-{self.fileno:04d}'))
        fileSink.process()

    def deep_copy(self):
        return None
        #source_clone = self.source.deep_copy()
        #f not source_clone:
        #    return None
        
        #return DirSink(source_clone, self.dir_path, self.sink_class, self.parms, self.fileno + 1)

        
