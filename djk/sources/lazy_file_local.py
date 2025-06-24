import gzip
import io
from typing import IO
from djk.sources.lazy_file import LazyFile

class LazyFileLocal(LazyFile):
    def __init__(self, path: str):
        self.path = path

    def open(self) -> IO[str]:
        raw = open(self.path, "rb")
        if self.path.endswith(".gz"):
            return io.TextIOWrapper(gzip.GzipFile(fileobj=raw))
        else:
            return io.TextIOWrapper(raw)

    def name(self) -> str:
        return self.path
