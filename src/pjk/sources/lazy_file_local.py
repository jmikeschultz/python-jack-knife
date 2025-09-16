# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import gzip
import io
from typing import IO
from pjk.sources.lazy_file import LazyFile

class LazyFileLocal(LazyFile):
    def __init__(self, path: str, is_gz: bool = False):
        self.path = path
        self.is_gz = is_gz

    def open(self) -> IO[str]:
        raw = open(self.path, "rb")
        if self.path.endswith(".gz") or self.is_gz:
            return io.TextIOWrapper(gzip.GzipFile(fileobj=raw))
        else:
            return io.TextIOWrapper(raw)

    def name(self) -> str:
        return self.path
