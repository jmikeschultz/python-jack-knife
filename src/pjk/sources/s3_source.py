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

    def __init__(
        self,
        s3_client,
        bucket: str,
        prefix: str,
        override_format: Optional[str],
        get_format_class_gz: Any,
    ):
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.override_format = override_format
        self.get_format_class_gz = get_format_class_gz

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

    def _build_source_for_key(self, key: str) -> Source:
        # Respect override format if provided.
        file_token = key if not self.override_format else f"{key}@format={self.override_format}"
        file_ptok = ParsedToken(file_token)

        format_class, is_gz = self.get_format_class_gz(file_ptok)
        if not format_class:
            raise RuntimeError(f"No format for file: {key}")

        #logger.info(f"S3Source starting s3://{self.bucket}/{key}")  

        lazy_file = LazyFileS3(self.bucket, key, is_gz)
        return format_class(lazy_file)

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
                # Reserve the next unit of work lazily.
                self._current = self._state.reserve_next_source()
                if self._current is None:
                    return  # exhausted

            try:
                # Delegate to the inner file Source (whatever format_class produced).
                yield from self._current
            finally:
                # Always move on to the next unit of work after finishing current.
                self._current = None

    def deep_copy(self):
        """
        Proactively reserve one unit of work for the clone so that multiple workers
        can start immediately without racing on the first item.
        """
        reserved = self._state.reserve_next_source()
        if reserved is None:
            return None
        return S3Source(self._state, reserved)

    @classmethod
    def create(cls, ptok: ParsedToken, get_format_class_gz: Any):
        """
        Returns immediately with a lazily-backed S3Source (no pre-enqueue).
        """
        import boto3 # lazy import
        s3_uri = ptok.all_but_params
        params = ptok.get_params()
        override = params.get("format")

        raw = s3_uri[3:]  # strip 's3:'
        raw = raw.removeprefix("//")
        bucket, _, prefix = raw.partition("/")

        # Build shared, lazy state. No listing performed yet.
        s3 = boto3.client("s3")
        shared = _SharedS3State(
            s3_client=s3,
            bucket=bucket,
            prefix=prefix,
            override_format=override,
            get_format_class_gz=get_format_class_gz,
        )

        # Return a source immediately (no blocking on S3).
        # If there are zero keys, the first __iter__ call will end cleanly.
        return cls(shared)
