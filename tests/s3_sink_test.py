# SPDX-License-Identifier: Apache-2.0
# Copyright 2025

import os
import json
import boto3
import pytest

from pjk.main import execute_tokens

import boto3
import json
import gzip
import io
from typing import List, Dict

def read_s3_json_records(s3_path: str) -> List[Dict]:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got {s3_path!r}")

    bucket_key = s3_path[len("s3://"):]
    bucket, sep, prefix = bucket_key.partition("/")
    if not sep:
        raise ValueError(f"S3 path must include a key or prefix: {s3_path}")

    s3 = boto3.client("s3")
    records: List[Dict] = []

    # Heuristic: if path has an extension (.json, .json.gz), treat as single file
    if prefix.endswith(".json") or prefix.endswith(".json.gz"):
        objects = [{"Key": prefix}]
    else:
        # Treat as folder/prefix
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        paginator = s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({"Key": obj["Key"]})

    for obj in objects:
        key = obj["Key"]
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()

        if key.endswith(".gz"):
            stream = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(body)), encoding="utf-8")
        else:
            stream = io.StringIO(body.decode("utf-8"))

        for line in stream:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    return records

def assert_records_match(inrecords, outrecords):
    """
    Assert two lists of dicts contain the same records, ignoring order.
    """
    inset = {json.dumps(r, sort_keys=True) for r in inrecords}
    outset = {json.dumps(r, sort_keys=True) for r in outrecords}
    print()
    print('in:', inset)
    print('out:', outset)
    assert inset == outset, f"Records differ!\nExpected: {inset}\nGot: {outset}"

def delete_s3_prefix(bucket: str, prefix: str):
    """
    Delete all objects under s3://bucket/prefix
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})

        # Batch delete in chunks of 1000 (API limit)
        if to_delete:
            for i in range(0, len(to_delete), 1000):
                chunk = {"Objects": to_delete[i : i + 1000]}
                s3.delete_objects(Bucket=bucket, Delete=chunk)


@pytest.mark.integration
def test_s3_sink_roundtrip(tmp_path):
    """
    Integration test for S3Sink:
      - execute_tokens writes an inline record to S3
      - boto3 fetches it back
      - assert the contents match
    """
    bucket = os.environ.get("PJK_TEST_BUCKET", 'stg-use1-adtech-temp')

    delete_s3_prefix(bucket, 'pjk-test')

    inrecs = [{'hello': 'world', 'num': 42}, {'up': 1}]
    s = json.dumps(inrecs)

    s3_path = f's3://{bucket}/pjk-test/test.json'
    execute_tokens([s, s3_path])
    outrecs = read_s3_json_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test.json.gz'
    execute_tokens([s, s3_path])
    outrecs = read_s3_json_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test_folder'
    execute_tokens([s, s3_path])
    outrecs = read_s3_json_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test_folder_gzipped'
    execute_tokens([s, f'{s3_path}@format=json.gz'])
    outrecs = read_s3_json_records(s3_path)
    assert_records_match(inrecs, outrecs)

    # fake folder with multiple files
    s3_path = f's3://{bucket}/pjk-test/test_folder2/file1.json'
    execute_tokens([s, s3_path])
    inrecs2 = [{'foo': 'bar'}]
    s2 = json.dumps(inrecs2)
    s3_path = f's3://{bucket}/pjk-test/test_folder2/file2.json'
    execute_tokens([s2, s3_path])
    outrecs = read_s3_json_records(f's3://{bucket}/pjk-test/test_folder2/')
    assert_records_match(inrecs + inrecs2, outrecs)