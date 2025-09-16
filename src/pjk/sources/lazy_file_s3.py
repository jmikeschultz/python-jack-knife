# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import io
import gzip
from typing import IO
from pjk.sources.lazy_file import LazyFile

class LazyFileS3(LazyFile):
    def __init__(self, bucket: str, key: str, is_gz: bool):
        import boto3 # lazy import
        self.s3 = boto3.client('s3') # for each thread
        self.bucket = bucket
        self.key = key
        self.is_gz = is_gz

    def open(self, binary=False) -> IO[str]:
        obj = self.s3.get_object(Bucket=self.bucket, Key=self.key)
        raw_body = obj['Body'].read()
        if self.is_gz:
            return io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw_body)))
        elif binary:
            return io.BytesIO(raw_body)
        else:
            return io.StringIO(raw_body.decode("utf-8"))

    def name(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
