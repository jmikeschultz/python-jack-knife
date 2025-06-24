from typing import Iterator
from djk.base import Source
from djk.sources.source_list import SourceListSource
from djk.sources.lazy_file_s3 import LazyFileS3
import boto3

class S3SourceFactory:
    def __init__(self, s3_uri: str, parms: str, resolve_method):
        raw = s3_uri[3:]
        self.parms = parms
        self.bucket, _, self.prefix = raw.partition('/')
        self.resolve_method = resolve_method
        self.s3 = boto3.client("s3")

    def _source_iter(self) -> Iterator[Source]:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                try:
                    source_cls = self.resolve_method(key, self.parms)
                    lazy_file = LazyFileS3(self.s3, self.bucket, key)
                    yield source_cls(lazy_file)
                except ValueError:
                    print(f"Skipping unsupported file: {key}")

    def create(self) -> SourceListSource:
        return SourceListSource(self._source_iter())
