import io
import gzip
from typing import IO
from djk.sources.lazy_file import LazyFile

class LazyFileS3(LazyFile):
    def __init__(self, s3_client, bucket: str, key: str):
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key

    def open(self) -> IO[str]:
        obj = self.s3.get_object(Bucket=self.bucket, Key=self.key)
        raw_body = obj['Body'].read()
        if self.key.endswith(".gz"):
            return io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw_body)))
        else:
            return io.StringIO(raw_body.decode("utf-8"))

    def name(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
