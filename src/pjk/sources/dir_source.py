# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import threading
from typing import Optional

from pjk.components import Source
from pjk.sources.lazy_file_local import LazyFileLocal
from pjk.log import logger


class DirSource(Source):
    """
    Iterate over files in a directory, materializing a concrete Source per file.
    Coordination between clones is handled by a shared file iterator protected
    by a lock. No queues, no is_root, no done_event.
    """
    extension = 'dir'  # ducklike hack so like FormatSource without the hassle

    def __init__(self, root: Source, file_iter = None, source_classes: dict = None, format_override: str = None):
        super().__init__(root=root) 
        self.current = None
        if not root: # WE! are the root
            if not file_iter:
                raise Exception('root creation must include file_iter')
            self.file_iter = file_iter
            self.iterator_lock = threading.Lock()
            self.format_override = format_override
            self.source_classes = source_classes

        else:
            self.file_iter = root.file_iter
            self.source_classes = root.source_classes
            self.format_override = root.format_override
            self.iterator_lock = root.iterator_lock

    # ---------------------------------------------------------------------
    # Iteration
    # ---------------------------------------------------------------------

    def __iter__(self):
        while True:
            if self.current is None:
                # Pull the next file-backed Source (skip unsupported files)
                self.current = self._get_next_source()
                if self.current is None:
                    return  # exhausted

            try:
                for record in self.current:
                    yield record
            finally:
                # move on after this inner source is exhausted
                self.current = None

    # ---------------------------------------------------------------------
    # Contention boundary: only here we touch the shared iterator
    # ---------------------------------------------------------------------

    # needed for in deep_clone to stop itereration
    def has_next(self):
        if self.current is not None:
            return True
        
        self.current = self._get_next_source()
        return self.current is not None

    def get_next_file(self) -> Optional[str]:
        """
        Thread-safe advancement of the shared file iterator.
        Returns the next file path, or None when exhausted.
        """
        with self.iterator_lock:
            if self.file_iter is None:
                return None
            try:
                path = next(self.file_iter)
                logger.debug(f'get_next_file -> {path}')
                return path
            except StopIteration:
                self.file_iter = None
                logger.debug('get_next_file -> None (exhausted)')
                return None

    def _get_next_source(self) -> Optional[Source]:
        """
        Keep drawing files until we either exhaust or we can construct a Source.
        """
        while True:
            file = self.get_next_file()
            if file is None:
                return None
            src = self._file_to_source(file)
            if src is None:
                logger.debug(f'skipping unsupported file: {file}')
                continue
            logger.debug(f'next source (from file) = {src}')
            return src

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    def _file_to_source(self, file: str) -> Optional[Source]:
        parts = file.split('.')
        is_gz = False

        if parts and parts[-1] == 'gz':
            is_gz = True
            parts.pop()

        fmt = parts[-1] if parts else None

        if self.format_override:
            fmt, is_gz = self.get_format_gz(self.format_override)

        if not fmt:
            return None

        source_class = self.source_classes.get(fmt)
        if not source_class:
            return None

        lazy_file = LazyFileLocal(file, is_gz)
        return source_class(lazy_file)

    def deep_copy(self):
        clone = DirSource(self)
        if clone.has_next():
            return clone
        else:
            return None

    # ---------------------------------------------------------------------
    # Class utilities
    # ---------------------------------------------------------------------

    @classmethod
    def get_format_gz(cls, input_str: str):
        is_gz = False
        fmt = input_str
        if input_str.endswith('.gz'):
            is_gz = True
            fmt = input_str[:-3]
        return fmt, is_gz

    @classmethod
    def _iter_files(cls, path: str, recursive: bool):
        if not recursive:
            for f in os.listdir(path):
                full = os.path.join(path, f)
                if os.path.isfile(full):
                    yield full
            return

        for dirpath, _, filenames in os.walk(path, topdown=True, followlinks=False):
            for name in filenames:
                full = os.path.join(dirpath, name)
                if os.path.isfile(full):
                    yield full

    @classmethod
    def create(
        cls,
        source_classes: dict,
        path_no_ext: str,
        format_override: Optional[str] = None,
        recursive: bool = False,
    ):
        """
        Factory: returns a DirSource that will lazily enumerate files.
        """
        file_iter = cls._iter_files(path_no_ext, recursive)

        return DirSource(
            root = None, # THIS is the root
            file_iter=file_iter,
            source_classes=source_classes,
            format_override=format_override
        )
