# SPDX-License-Identifier: Apache-2.0
# Copyright 2025

import io
import time
import boto3
from botocore.exceptions import BotoCoreError, ClientError


class S3MultipartWriter(io.RawIOBase):
    """
    File-like writer for S3.
    - Buffers to memory until >= part_size (default 8 MB).
    - If total < 5 MB → upload once via put_object.
    - If >= part_size → stream parts with multipart upload.
    """

    def __init__(self, bucket, key, part_size=8 * 1024 * 1024, max_retries=5):
        super().__init__()
        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.key = key
        self.part_size = max(5 * 1024 * 1024, int(part_size))
        self.max_retries = max(1, int(max_retries))

        self.upload_id = None
        self.buffer = bytearray()
        self.parts = []
        self.part_number = 1
        self._closed = False

    @property
    def closed(self):
        return self._closed

    def writable(self):
        return True

    def write(self, b: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not isinstance(b, (bytes, bytearray)):
            raise TypeError("write() requires bytes-like object")

        self.buffer.extend(b)

        # If buffer is bigger than part_size, start multipart
        while len(self.buffer) >= self.part_size:
            self._ensure_multipart()
            self._flush_part()

        return len(b)

    def flush(self):
        # noop; final flush handled in close()
        return

    def close(self):
        if self._closed:
            return
        try:
            if self.upload_id is None:
                # never started multipart → upload all bytes directly
                body = bytes(self.buffer)
                self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=body)
            else:
                # multipart case → flush remaining as final part
                if self.buffer:
                    self._flush_part(final=True)

                self.s3.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={
                        "Parts": [
                            {"ETag": p["ETag"], "PartNumber": p["PartNumber"]}
                            for p in self.parts
                        ]
                    },
                )
        except Exception:
            self.abort()
            raise
        finally:
            self._closed = True
            super().close()

    def _ensure_multipart(self):
        if self.upload_id is None:
            resp = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)
            self.upload_id = resp["UploadId"]

    def _flush_part(self, final=False):
        if not self.buffer:
            return

        if final:
            part = bytes(self.buffer)
            self.buffer.clear()
        else:
            part = bytes(self.buffer[: self.part_size])
            del self.buffer[: self.part_size]

        retries = 0
        while True:
            try:
                resp = self.s3.upload_part(
                    Bucket=self.bucket,
                    Key=self.key,
                    PartNumber=self.part_number,
                    UploadId=self.upload_id,
                    Body=part,
                )
                self.parts.append(
                    {"ETag": resp["ETag"], "PartNumber": self.part_number}
                )
                self.part_number += 1
                return
            except (BotoCoreError, ClientError):
                retries += 1
                if retries >= self.max_retries:
                    self.abort()
                    raise
                time.sleep(2**retries)

    def abort(self):
        if self.upload_id:
            try:
                self.s3.abort_multipart_upload(
                    Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
                )
            except Exception:
                pass
