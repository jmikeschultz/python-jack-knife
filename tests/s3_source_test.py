# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os, json, sys
from pjk.main import execute_tokens
from s3_sink_test import delete_s3_prefix, assert_records_match
import boto3, json, gzip, io
from typing import List, Dict
import csv

def write_s3_records(s3_path: str, records: List[Dict]):
    """
    Write list of dicts to S3 in one of the supported formats:
      - .json / .json.gz → newline-delimited JSON (NDJSON)
      - .csv / .csv.gz   → CSV with headers from dict keys
      - .tsv / .tsv.gz   → TSV with headers from dict keys

    Compression is applied automatically if the extension ends with .gz.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got {s3_path!r}")

    bucket_key = s3_path[len("s3://"):]
    bucket, _, key = bucket_key.partition("/")
    if not key:
        raise ValueError(f"S3 path must include a key: {s3_path}")

    # Detect compression
    is_gz = key.endswith(".gz")
    bare_key = key[:-3] if is_gz else key

    if bare_key.endswith(".json"):
        # NDJSON
        body = "\n".join(json.dumps(r) for r in records) + "\n"
        payload = body.encode("utf-8")

    elif bare_key.endswith(".csv") or bare_key.endswith(".tsv"):
        # CSV/TSV with header
        delimiter = "," if bare_key.endswith(".csv") else "\t"
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=records[0].keys(), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(records)
        payload = buf.getvalue().encode("utf-8")

    else:
        raise ValueError(f"Unsupported format for key: {key}")

    # Apply gzip if needed
    if is_gz:
        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(payload)
        payload = gz_buf.getvalue()

    # Upload
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=payload)


def write_s3_json_records(s3_path: str, records: List[Dict]):
    """
    Write list of dicts to S3 as newline-delimited JSON (NDJSON).
    If path ends with .gz, data is gzipped.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got {s3_path!r}")

    bucket_key = s3_path[len("s3://"):]
    bucket, _, key = bucket_key.partition("/")
    if not key:
        raise ValueError(f"S3 path must include a key: {s3_path}")

    body = "\n".join(json.dumps(r) for r in records) + "\n"

    if key.endswith(".gz"):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(body.encode("utf-8"))
        payload = buf.getvalue()
    else:
        payload = body.encode("utf-8")

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=payload)

def source_records_and_verify(s3_path, expected):
    """
    Run pipeline from s3_path → local temp JSON file, load records,
    and compare to expected using assert_records_match (normalizes ints).
    """
    execute_tokens([s3_path, '/tmp/outtest.json'])

    with open('/tmp/outtest.json') as f:
        actual = [json.loads(line) for line in f]

    assert_records_match(expected, actual)


def format_roundtrip(format):
    """
    Integration test for S3Source:
      - boto3 helper writes known records into S3
      - pjk executes with S3Source to pull them back out
      - compare to input records
    """
    bucket = os.environ.get("PJK_TEST_BUCKET")
    if not bucket:
        print('need to set PJK_TEST_BUCKET environment variable')
        sys.exit(-1)
    delete_s3_prefix(bucket, "pjk-test-source")

    # using records all with same schema because of tsv and csv
    inrecs = [{'hello': 'world', 'num': 42}, {'hello': f'{format}', 'num': 254}]

    # Single file
    s3_path = f"s3://{bucket}/pjk-test-source/test.{format}"
    write_s3_records(s3_path, inrecs)
    source_records_and_verify(s3_path, inrecs)

    # single gzipped file
    s3_path = f"s3://{bucket}/pjk-test-source/test.{format}.gz"
    write_s3_records(s3_path, inrecs)
    source_records_and_verify(s3_path, inrecs)

    # Folder prefix
    s3_path = f"s3://{bucket}/pjk-test-source/folder"
    s3_file1 = f"{s3_path}/file1.{format}"
    s3_file2 = f"{s3_path}/file2.{format}"
    write_s3_records(s3_file1, inrecs)
    inrecs2 = [{'hello': 'you', 'num': 1}]
    write_s3_records(s3_file2, inrecs2)
    source_records_and_verify(s3_path, inrecs + inrecs2)

def test_rountrips():
    format_roundtrip('json')
    format_roundtrip('csv') 
    format_roundtrip('tsv')