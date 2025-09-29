# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2025 Mike Schultz

from threading import Lock
from typing import Optional, Any, Iterator, Tuple
from pjk.base import Source, ParsedToken
from pjk.sources.lazy_file_s3 import LazyFileS3
from pjk.log import logger

class _SharedS3State:
    """
    Shared, thread-safe lazy iterator over S3 objects for a given bucket/prefix.
    All S3Source instances created via deep_copy() share this state so that:
      - Keys are produced lazily (no initial drain into a queue).
      - Each consumer reserves distinct work atomically.
    """

    def __init__(self, s3_client, bucket: str, prefix: str, sources: dict, format_override: str):
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.sources = sources
        self.format_override = format_override

        # Build a *single* lazy iterator over keys from the paginator.
        self._key_iter = self._iter_s3_keys()
        self._lock = Lock()
        self._exhausted = False  # explicit flag; avoids extra paginator calls after completion

    def _iter_s3_keys(self) -> Iterator[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        # Paginate lazily; do not force iteration here.
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            contents = page.get("Contents", [])
            # Preserve original S3 ordering within each page.
            for obj in contents:
                # Defensive: ensure Key exists and is str
                key = obj.get("Key")
                if isinstance(key, str) and key:
                    yield key

    def get_format_gz(cls, input:str):
        is_gz = False
        format = input
        if input.endswith('.gz'):
            is_gz = True
            format = input[:-3]
        return format, is_gz

    def _build_source_for_key(self, key: str) -> Source:
        #logger.info(f"S3Source starting s3://{self.bucket}/{key}")  

        parts = key.split('.')
        is_gz = False

        if parts[-1] == 'gz':
            is_gz = True
            parts.pop()

        format = parts[-1] if self.format_override is None else self.format_override

        if self.format_override:
            format, is_gz = self.get_format_gz(self.format_override)

        source_class = self.sources.get(format)
        lazy_file = LazyFileS3(self.bucket, key, is_gz)
        return source_class(lazy_file)

    def reserve_next_source(self) -> Optional[Source]:
        """
        Atomically reserve and construct the next file-backed Source.
        Returns None when the iterator is exhausted.
        """
        if self._exhausted:
            return None

        with self._lock:
            if self._exhausted:
                return None
            try:
                key = next(self._key_iter)
            except StopIteration:
                self._exhausted = True
                return None

        # Construct outside the lock to minimize critical section time.
        return self._build_source_for_key(key)


class S3Source(Source):
    """
    A Source that draws from a shared, lazy S3 key stream.
    - Iteration pulls a new inner Source on demand.
    - deep_copy() proactively reserves one unit of work for the clone, mirroring your queue split.
    """

    def __init__(self, shared_state: _SharedS3State, reserved: Optional[Source] = None):
        self._state = shared_state
        self._current: Optional[Source] = reserved

    def __iter__(self):
        while True:
            if self._current is None:
                self._current = self._state.reserve_next_source()
                if self._current is None:
                    return  # exhausted

            # Explicitly iterate current once, then discard
            for record in self._current:
                yield record

            # Move to next file
            self._current = None

    def deep_copy(self):
        """
        Proactively reserve one unit of work for the clone so that multiple workers
        can start immediately without racing on the first item.
        """
        reserved = self._state.reserve_next_source()
        if reserved is None:
            return None
        return self.__class__(self._state, reserved)
    
    @classmethod
    def create(cls, sources, path_no_ext: str,  ext: str, format_override: str = None):
        import boto3 # lazy import
        path_no_ext = path_no_ext if not path_no_ext.startswith('//') else path_no_ext[2:]
        bucket, _, prefix = path_no_ext.partition("/")
        prefix = prefix if ext is None else f'{prefix}.{ext}'

        s3 = boto3.client("s3")
        state = _SharedS3State(s3_client=s3, bucket=bucket, prefix=prefix,
                                sources=sources, format_override=format_override)
        reserved = state.reserve_next_source()
        return cls(state, reserved)