# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os, gzip, shutil
from pjk.base import Sink, ParsedToken, Usage
from typing import Optional, Type
from .format_sink import Sink
from pjk.log import logger
import gzip

class DirSink(Sink):
    def __init__(self, sink_class: Type[Sink], path_no_ext: str, is_gz: bool, fileno: int = 0):
        super().__init__(None, None)
        self.sink_class = sink_class
        self.path_no_ext = path_no_ext
        self.is_gz = is_gz
        self.fileno = fileno
        self.num_files = 1

        if os.path.isdir(self.path_no_ext):
            # remove everything inside
            for entry in os.listdir(self.path_no_ext):
                full = os.path.join(self.path_no_ext, entry)
            if os.path.isfile(full) or os.path.islink(full):
                os.unlink(full)
            elif os.path.isdir(full):
                shutil.rmtree(full)
        else:
            os.makedirs(self.path_no_ext, exist_ok=True)

    def process(self):
        # build the base filename
        base = os.path.join(self.path_no_ext, f"file-{self.fileno:04d}")

        # include extension here (format sink name + gz logic)
        filename = f"{base}.{self.sink_class.extension}"
        if self.is_gz:
            filename += ".gz"

        # open output file handle
        outfile = gzip.open(filename, "wt", encoding="utf-8") if self.is_gz else open(filename, "wt", encoding="utf-8")

        # create the format-specific sink with the open handle
        file_sink = self.sink_class(outfile)
        file_sink.add_source(self.input)

        logger.debug(f"in process sinking to local file: {filename}")
        file_sink.process()
        outfile.close()

    def deep_copy(self):
        # Ask the upstream source to duplicate itself
        source_clone = self.input.deep_copy()
        if source_clone is None:
            return None

        # Create a new DirSink with the next file index
        clone = DirSink(
            sink_class=self.sink_class,
            path_no_ext=self.path_no_ext,
            is_gz=self.is_gz,
            fileno=self.num_files,
        )

        # Wire up the cloned source to the new sink
        clone.add_source(source_clone)

        # Increment file counter for the next clone
        self.num_files += 1
        return clone

