# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 Mike Schultz

import io
import gzip
from typing import Optional, Type
from pjk.base import Source, Sink
from pjk.log import logger
from pjk.sinks.s3_stream import S3MultipartWriter


class S3Sink(Sink):
    """
    Write records to S3 in the given <format>.

    - Folder mode (path without extension):
        s3:bucket/prefix/ → file-0000.ext, file-0001.ext, ...
    - Single-file mode (path ends with .ext or .ext.gz):
        s3:bucket/prefix/output.csv[.gz]

    Args (via Usage):
      - path: 'bucket/path/to/files' (bucket required, prefix optional)
    """

    _FILENAME_BASE: str = "file"
    _FILENAME_DIGITS: int = 4
    
    def __init__(self, sink_class: Type[Sink], path_no_ext: str, is_gz: bool, fileno: int):
        self.path_no_ext = path_no_ext if not path_no_ext.startswith('//') else path_no_ext[2:] # strip leading //
        self.sink_class = sink_class
        self.is_gz = is_gz
        self.fileno = fileno
        self.is_single_file = fileno == -1
        if self.path_no_ext.endswith('/') and not self.is_single_file:
            self.path_no_ext = self.path_no_ext[:-1]

        self.num_files = 1

    def _build_object_key(self, index: int) -> str:
        if self.is_single_file:
            file_name = f'{self.path_no_ext}.{self.sink_class.extension}'
        else:
            file_name = f"{self.path_no_ext}/{self._FILENAME_BASE}-{index:0{self._FILENAME_DIGITS}d}.{self.sink_class.extension}"

        if self.is_gz:
            file_name += ".gz"

        return file_name

    def process(self):
        object_key = self._build_object_key(self.fileno)
        bucket, key = object_key.split("/", 1)

        with S3MultipartWriter(bucket, key) as writer:
            if self.is_gz:
                # gzip needs a binary sink → use writer directly
                with gzip.GzipFile(fileobj=writer, mode="wb") as gz:
                    with io.TextIOWrapper(gz, encoding="utf-8", newline="") as outfile:
                        file_sink = self.sink_class(outfile)
                        file_sink.add_source(self.input)
                        logger.debug(f"S3Sink streaming GZ to s3://{bucket}/{key}")
                        file_sink.process()
            else:
                # plain text path
                with io.TextIOWrapper(writer, encoding="utf-8", newline="") as outfile:
                    file_sink = self.sink_class(outfile)
                    file_sink.add_source(self.input)
                    logger.debug(f"S3Sink streaming to s3://{bucket}/{key}")
                    file_sink.process()


    def deep_copy(self):
        if self.is_single_file:
            # Single-file mode: no fanout allowed
            return None

        source_clone: Optional[Source] = self.input.deep_copy()
        if not source_clone:
            return None

        clone = S3Sink(
            sink_class=self.sink_class,
            path_no_ext=self.path_no_ext,
            is_gz=self.is_gz,
            fileno=self.num_files,
        )
        clone.add_source(source_clone)

        self.num_files += 1
        return clone
