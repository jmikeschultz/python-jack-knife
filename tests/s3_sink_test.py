# SPDX-License-Identifier: Apache-2.0
# Copyright 2025

import os
import json
import boto3
from pjk.main import execute_tokens
import gzip
import io
from typing import List, Dict
import csv

def read_s3_records(s3_path: str) -> List[Dict]:
    """
    Read one or more S3 objects (JSON lines, CSV, or TSV, plain or gzipped)
    and return a flat list of dicts.

    Format is inferred from the file extension.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got {s3_path!r}")

    bucket_key = s3_path[len("s3://"):]
    bucket, sep, prefix = bucket_key.partition("/")
    if not sep:
        raise ValueError(f"S3 path must include a key or prefix: {s3_path}")

    s3 = boto3.client("s3")
    records: List[Dict] = []

    # Determine if single file or folder prefix
    if any(
        prefix.endswith(ext)
        for ext in [".json", ".json.gz", ".csv", ".csv.gz", ".tsv", ".tsv.gz"]
    ):
        objects = [{"Key": prefix}]
    else:
        if not prefix.endswith("/"):
            prefix += "/"
        paginator = s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({"Key": obj["Key"]})

    for obj in objects:
        key = obj["Key"]
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()

        # Handle compression
        if key.endswith(".gz"):
            stream = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(body)), encoding="utf-8")
            bare_key = key[:-3]  # strip ".gz"
        else:
            stream = io.StringIO(body.decode("utf-8"))
            bare_key = key

        # Detect format from extension
        if bare_key.endswith(".json"):
            for line in stream:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        elif bare_key.endswith(".csv"):
            reader = csv.DictReader(stream, delimiter=",")
            for row in reader:
                records.append(dict(row))
        elif bare_key.endswith(".tsv"):
            reader = csv.DictReader(stream, delimiter="\t")
            for row in reader:
                records.append(dict(row))
        else:
            raise ValueError(f"Unsupported format for key: {key}")

    return records

def _normalize(record):
    return {k: int(v) if isinstance(v, str) and v.isdigit() else v
            for k, v in record.items()}

def assert_records_match(inrecords, outrecords):
    inset = {json.dumps(_normalize(r), sort_keys=True) for r in inrecords}
    outset = {json.dumps(_normalize(r), sort_keys=True) for r in outrecords}
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


def format_roundtrip(format):
    """
    Integration test for S3Sink:
      - execute_tokens writes an inline record to S3
      - boto3 fetches it back
      - assert the contents match
    """
    bucket = os.environ.get("PJK_TEST_BUCKET", 'stg-use1-adtech-temp')

    delete_s3_prefix(bucket, 'pjk-test')

    inrecs = [{'hello': 'world', 'num': 42}, {'hello': 'there', 'num': 254}]
    s = json.dumps(inrecs)

    s3_path = f's3://{bucket}/pjk-test/test.{format}'
    execute_tokens([s, s3_path])
    outrecs = read_s3_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test.{format}.gz'
    execute_tokens([s, s3_path])
    outrecs = read_s3_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test_folder2'
    execute_tokens([s, f'{s3_path}@format={format}'])
    outrecs = read_s3_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test_folder'
    execute_tokens([s, s3_path])
    outrecs = read_s3_records(s3_path)
    assert_records_match(inrecs, outrecs)

    s3_path = f's3://{bucket}/pjk-test/test_folder_gzipped'
    execute_tokens([s, f'{s3_path}@format={format}.gz'])
    outrecs = read_s3_records(s3_path)
    assert_records_match(inrecs, outrecs)

    # fake folder with multiple files
    s3_path = f's3://{bucket}/pjk-test/test_folder2/file1.{format}'
    execute_tokens([s, s3_path])
    inrecs2 = [{'hello': 'you', 'num': 1}]
    s2 = json.dumps(inrecs2)
    s3_path = f's3://{bucket}/pjk-test/test_folder2/file2.{format}'
    execute_tokens([s2, s3_path])
    outrecs = read_s3_records(f's3://{bucket}/pjk-test/test_folder2/')
    assert_records_match(inrecs + inrecs2, outrecs)

def test_rountrips():
    format_roundtrip('json')
    format_roundtrip('csv') 
    format_roundtrip('tsv')
