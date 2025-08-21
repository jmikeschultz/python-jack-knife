# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import boto3
from queue import Queue, Empty
from typing import Optional, Any
from djk.base import Source, ParsedToken
from djk.sources.lazy_file_s3 import LazyFileS3

class S3Source(Source):
    def __init__(self, source_queue: Queue, in_source: Optional[Source] = None):
        self.source_queue = source_queue
        self.current = in_source

    def __iter__(self):
        while True:
            if self.current is None:
                try:
                    self.current = self.source_queue.get_nowait()
                except Empty:
                    return  # done

            try:
                yield from self.current
            finally:
                self.current = None  # move on to next source

    def deep_copy(self):
        if self.source_queue.qsize() <= 1:
            return None
        try:
            next_source = self.source_queue.get_nowait()
        except Empty:
            return None

        return S3Source(self.source_queue, next_source)

    @classmethod
    def create(cls, ptok: ParsedToken, get_format_class_gz: Any):
        s3_uri = ptok.all_but_params
        params = ptok.get_params()
        override = params.get('format')

        raw = s3_uri[3:]  # strip 's3:'
        raw = raw.removeprefix('//')
        bucket, _, prefix = raw.partition('/')

        s3 = boto3.client("s3")
        source_queue = Queue()

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                file_token = key if not override else f"{key}@format={override}"
                file_ptok = ParsedToken(file_token)

                format_class, is_gz = get_format_class_gz(file_ptok)
                if format_class:
                    lazy_file = LazyFileS3(bucket, key, is_gz)
                    source_queue.put(format_class(lazy_file))
                else:
                    raise RuntimeError(f"No format for file: {key}")

        if source_queue.empty():
            return None

        return cls(source_queue)
