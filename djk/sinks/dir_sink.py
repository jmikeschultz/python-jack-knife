import os
from djk.base import Source, Sink
from djk.log import logger

class DirSink(Sink):
    def __init__(self, source: Source, path: str, sink_class: str, parms: str, fileno: int = 0):
        self.source = source
        self.dir_path = path
        self.sink_class = sink_class
        self.parms = parms
        self.fileno = fileno
        self.num_files = 1

    def process(self):
        file = os.path.join(self.dir_path, f'file-{self.fileno:04d}')
        fileSink = self.sink_class(self.source, file)
        logger.debug(f'in process sinking to: {file}')
        fileSink.process()

    def deep_copy(self):
        source_clone = self.source.deep_copy()
        if not source_clone:
            return None
        
        self.num_files += 1
        return DirSink(source_clone, self.dir_path, self.sink_class, self.parms, self.num_files-1)

        
