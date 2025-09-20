# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 Mike Schultz

import io
import boto3, time
from botocore.exceptions import BotoCoreError, ClientError


class S3MultipartWriter(io.RawIOBase):
    """
    File-like writer that streams to S3 using multipart upload.
    - Buffers until part_size, then uploads a part.
    - On close, flushes any remaining data (even < part_size).
    - If no parts were uploaded, falls back to a simple put_object.
    """

    def __init__(self, bucket, key, part_size=8 * 1024 * 1024, max_retries=5):
        super().__init__()
        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.key = key
        self.part_size = part_size
        self.max_retries = max_retries

        resp = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        self.upload_id = resp["UploadId"]
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
        self.buffer.extend(b)
        while len(self.buffer) >= self.part_size:
            self._flush_part()
        return len(b)

    def flush(self):
        if self._closed:
            return
        # upload any remaining buffer as a final part
        if self.buffer:
            self._flush_part(final=True)

    def close(self):
        if self._closed:
            return
        try:
            self.flush()
            if self.parts:
                # normal multipart completion
                self.s3.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={"Parts": self.parts},
                )
            else:
                # no multipart parts uploaded, just do a simple put_object
                body = bytes(self.buffer) if self.buffer else b""
                self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=body)
        except Exception:
            self.abort()
            raise
        finally:
            self._closed = True
            super().close()

    def _flush_part(self, final=False):
        if not self.buffer:
            return
        # for final flush, upload everything, even if smaller than part_size
        if final:
            part = bytes(self.buffer)
            self.buffer.clear()
        else:
            part = bytes(self.buffer[: self.part_size])
            self.buffer = self.buffer[self.part_size :]

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
        try:
            self.s3.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
            )
        except Exception:
            pass
