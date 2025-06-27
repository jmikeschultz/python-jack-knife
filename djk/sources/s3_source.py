import boto3
from queue import Queue, Empty
from typing import Iterator, Optional
from djk.base import Source
from djk.sources.lazy_file_s3 import LazyFileS3

class S3Source(Source):
    def __init__(self, source_queue: Queue, in_source: Optional[Source] = None):
        self.source_queue = source_queue
        self.current = in_source

    def next(self):
        while True:
            if self.current is None:
                try:
                    self.current = self.source_queue.get_nowait()
                except Empty:
                    return None

            record = self.current.next()
            if record is not None:
                return record
            else:
                self.current = None

    def deep_copy(self):
        if self.source_queue.qsize() <= 1:
            return None # leave for original S3Source
        try:
            next_source = self.source_queue.get_nowait()
        except Empty:
            return None

        return S3Source(self.source_queue, next_source)

    @classmethod
    def create(cls, s3_uri: str, source_class_getter, parms: str = ""):
        raw = s3_uri[3:]  # strip 's3:'
        bucket, _, prefix = raw.partition('/')
        s3 = boto3.client("s3")
        source_queue = Queue()

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                source_cls = source_class_getter(key, parms)
                if source_cls:
                    lazy_file = LazyFileS3(s3, bucket, key)
                    source_queue.put(source_cls(lazy_file))
                else:
                    print(f"fix me in source factory: {key}")
                    break

        if source_queue.empty():
            return None

        return cls(source_queue)
