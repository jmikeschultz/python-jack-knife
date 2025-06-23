import boto3
import json
import gzip
import io
from typing import Optional, Dict, Iterator
from djk.base import Source
from djk.sources.json_source import JsonSource

PARAMS_DELIM = ";"

class S3Source(Source):
    def __init__(self, s3_uri: str):
        """
        Example:
            s3:my-bucket/path/to/data;format=json
        """
        assert s3_uri.startswith("s3:"), f"Invalid S3 URI: {s3_uri}"
        self.bucket, self.prefix, self.options = self._parse_uri(s3_uri[3:])
        self.format = self.options.get("format")  # optional override

        self.s3 = boto3.client("s3")
        self.file_keys = self._list_keys()
        self.key_iter = iter(self.file_keys)
        self.current_source: Optional[Source] = None

    def _parse_uri(self, raw: str):
        if PARAMS_DELIM in raw:
            path, *param_parts = raw.split(PARAMS_DELIM)
            options = dict(
                part.split("=", 1) for part in param_parts if "=" in part
            )
        else:
            path = raw
            options = {}

        bucket, _, prefix = path.partition("/")
        return bucket, prefix, options

    def _list_keys(self):
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                fmt = self._detect_format(key)
                if fmt == "json":
                    keys.append(key)
                else:
                    print(f"⚠️ Skipping unsupported file: {key}")
        return keys

    def _get_s3_stream(self, key: str):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        raw_body = obj['Body'].read()
        if key.endswith(".gz"):
            return io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw_body)))
        return io.StringIO(raw_body.decode("utf-8"))

    def _detect_format(self, key: str) -> Optional[str]:
        if self.format:
            return self.format
        if key.endswith(".json") or key.endswith(".json.gz") or key.endswith(".log.gz"):
            return "json"
        return None

    def _next_source(self) -> Optional[Source]:
        for key in self.key_iter:
            try:
                fileobj = self._get_s3_stream(key)
                return JsonSource(fileobj)
            except Exception as e:
                print(f"⚠️ Failed to open file {key}: {e}")
                continue
        return None

    def next(self) -> Optional[dict]:
        while True:
            if self.current_source is None:
                self.current_source = self._next_source()
                if self.current_source is None:
                    return None  # no more sources

            record = self.current_source.next()
            if record is not None:
                return record
            else:
                self.current_source = None  # exhausted this file
