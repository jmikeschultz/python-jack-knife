# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 Mike Schultz

from typing import Optional, Type
from pjk.base import Source, Sink, ParsedToken, Usage
from pjk.log import logger


class S3Sink(Sink):
    """
    Write records to S3 in the given <format>, partitioned into:
      s3:{bucket}/{prefix}/file-0000
      s3:{bucket}/{prefix}/file-0001
    Args (via Usage):
      - path: 'bucket/path/to/files' (bucket required, prefix optional)
    """

    _FILENAME_BASE: str = "file"
    _FILENAME_DIGITS: int = 4
    _SCHEME: str = "s3:"

    @classmethod
    def usage(cls):
        usage = Usage(
            name="<format>",
            desc="Write records to S3 in the given <format> (e.g., csv)",
            component_class=cls,
        )
        usage.def_arg(name="path", usage="bucket/path/to/files")
        return usage

    def __init__(
        self,
        ptok: ParsedToken,
        usage: Usage,
        sink_class: Type[Sink],
        is_gz: bool,
        fileno: int = 0,
    ):
        super().__init__(ptok, usage)

        raw_path: Optional[str] = usage.get_arg("path")
        if not raw_path:
            raise ValueError("S3Sink requires 'path' argument like 'bucket/path/to/files'")

        # Normalize: allow 's3:bucket/...' or '/bucket/...', strip extras
        path = raw_path.strip()
        if path.startswith(self._SCHEME):
            path = path[len(self._SCHEME) :]
        path = path.lstrip("/")

        # Ensure a trailing slash so we can append filenames cleanly
        self.base_path: str = path if path.endswith("/") else path + "/"

        self.ptok = ptok
        self.usage = usage
        self.sink_class = sink_class
        self.is_gz = is_gz
        self.fileno = fileno
        self.num_files = 1  # next file index for deep_copy clones

    def _build_object_key(self, index: int) -> str:
        file_name = f"{self._FILENAME_BASE}-{index:0{self._FILENAME_DIGITS}d}"
        return f"{self.base_path}{file_name}"

    def _build_parsed_token_for_index(self, index: int) -> ParsedToken:
        key = self._build_object_key(index)
        token_str = f"{self._SCHEME}{key}:{self.is_gz}"
        return ParsedToken(token_str)

    def process(self):
        file_ptok = self._build_parsed_token_for_index(self.fileno)

        file_usage = self.sink_class.usage()
        file_usage.bind(file_ptok)

        file_sink = self.sink_class(file_ptok, file_usage)
        file_sink.add_source(self.input)

        logger.debug(
            f"in process sinking to: s3:{self.base_path} (object index {self.fileno:0{self._FILENAME_DIGITS}d})"
        )
        file_sink.process()

    def deep_copy(self):
        source_clone: Optional[Source] = self.input.deep_copy()
        if not source_clone:
            return None

        clone = S3Sink(
            ptok=self.ptok,
            usage=self.usage,
            sink_class=self.sink_class,
            is_gz=self.is_gz,
            fileno=self.num_files,
        )
        clone.add_source(source_clone)

        self.num_files += 1
        return clone
