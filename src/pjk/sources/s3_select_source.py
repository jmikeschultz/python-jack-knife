# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import re
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Tuple

import yaml

from pjk.components import Source
from pjk.usage import ParsedToken, Usage


# ============================================================
#  Per-object S3 Select reader
# ============================================================

class S3SelectObjectSource(Source):
    """
    Runs S3 Select on a single S3 key and streams JSON rows.
    """

    def __init__(
        self,
        s3_client,
        bucket: str,
        key: str,
        query: str,
        input_format: str,
        is_gz: bool,
    ):
        super().__init__(root=None)
        self._s3 = s3_client
        self._bucket = bucket
        self._key = key
        self._query = query
        self._input_format = input_format
        self._is_gz = is_gz

    def _build_input_serialization(self) -> Dict[str, Any]:
        fmt = self._input_format.lower()

        if fmt == "json":
            base = {"JSON": {"Type": "LINES"}}
        elif fmt == "csv":
            base = {"CSV": {"FileHeaderInfo": "USE", "FieldDelimiter": ","}}
        elif fmt == "tsv":
            base = {"CSV": {"FileHeaderInfo": "USE", "FieldDelimiter": "\t"}}
        elif fmt == "parquet":
            base = {"Parquet": {}}
        else:
            raise ValueError(f"Unsupported input format for S3 Select: {self._input_format}")

        if self._is_gz:
            base["CompressionType"] = "GZIP"

        return base

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        input_ser = self._build_input_serialization()
        output_ser = {"JSON": {}}  # JSON objects per record

        resp = self._s3.select_object_content(
            Bucket=self._bucket,
            Key=self._key,
            ExpressionType="SQL",
            Expression=self._query,
            InputSerialization=input_ser,
            OutputSerialization=output_ser,
        )

        decoder = json.JSONDecoder()
        buffer = ""

        for event in resp["Payload"]:
            if "Records" not in event:
                continue

            chunk = event["Records"]["Payload"].decode("utf-8")
            buffer += chunk

            # peel off as many complete JSON objects as we can
            while True:
                stripped = buffer.lstrip()
                if not stripped:
                    buffer = ""
                    break

                try:
                    obj, end = decoder.raw_decode(stripped)
                except json.JSONDecodeError:
                    # incomplete JSON; wait for more data
                    break

                yield obj
                buffer = stripped[end:]


# ============================================================
#  Shared S3 Select state (prefix iteration + threading)
# ============================================================

class _SharedS3SelectState:
    """
    Shared, thread-safe lazy iterator over S3 objects for S3 Select queries.

    Driven by a .s3s YAML config that specifies:
      - s3_bucket
      - prefix
      - optional sub_keys: [ "01", "02", ... ]
      - optional key_regex
      - format
      - query
    """

    def __init__(
        self,
        s3_client,
        bucket: str,
        prefixes: List[str],
        format_override: str,
        query: str,
        key_regex: Optional[str] = None,
    ):
        self.s3 = s3_client
        self.bucket = bucket
        self.prefixes = prefixes
        self.format_override = format_override
        self.query = query

        self._key_regex = re.compile(key_regex) if key_regex else None

        self._lock = Lock()
        self._exhausted = False
        self._prefix_index = 0
        self._current_iter: Optional[Iterator[str]] = None

    @staticmethod
    def _get_format_gz(value: str) -> Tuple[str, bool]:
        """
        Split 'json.gz' -> ('json', True), 'json' -> ('json', False), etc.
        """
        is_gz = value.endswith(".gz")
        fmt = value[:-3] if is_gz else value
        return fmt, is_gz

    def _next_key_iter(self) -> Optional[Iterator[str]]:
        """
        Get an iterator over keys for the next prefix in self.prefixes.
        """
        if self._prefix_index >= len(self.prefixes):
            return None

        prefix = self.prefixes[self._prefix_index]
        self._prefix_index += 1

        paginator = self.s3.get_paginator("list_objects_v2")
        return (
            obj.get("Key")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
            if isinstance(obj.get("Key"), str)
        )

    def _iter_all_keys(self) -> Iterator[str]:
        """
        Iterate over all keys for all prefixes, applying key_regex if present.
        """
        while True:
            if self._current_iter is None:
                self._current_iter = self._next_key_iter()
                if self._current_iter is None:
                    return

            try:
                key = next(self._current_iter)
            except StopIteration:
                self._current_iter = None
                continue

            if self._key_regex and not self._key_regex.search(key):
                continue

            yield key

    def _infer_format_and_compression(self) -> Tuple[str, bool]:
        if not self.format_override:
            raise ValueError("format is required in .s3s config")
        fmt, is_gz = self._get_format_gz(self.format_override)
        return fmt, is_gz

    def _build_source_for_key(self, key: str) -> Source:
        fmt, is_gz = self._infer_format_and_compression()
        return S3SelectObjectSource(
            s3_client=self.s3,
            bucket=self.bucket,
            key=key,
            query=self.query,
            input_format=fmt,
            is_gz=is_gz,
        )

    def reserve_next_source(self) -> Optional[Source]:
        """
        Atomically reserve and construct the next per-key S3SelectObjectSource.
        Returns None when exhausted.
        """
        if self._exhausted:
            return None

        with self._lock:
            if self._exhausted:
                return None

            try:
                key = next(self._iter_all_keys())
            except StopIteration:
                self._exhausted = True
                return None

        return self._build_source_for_key(key)


# ============================================================
#  Main S3 Select Source (YAML-driven)
# ============================================================

class S3SelectSource(Source):
    """
    S3 Select source using a .s3s YAML config file.

    Example config.s3s:

        s3_bucket: my-bucket
        prefix: balancerevent/2025/11/
        sub_keys:
          - 01
          - 02
        format: json.gz
        query: |
          SELECT ...
          FROM S3Object s

    Parser passes the config file path as ptok.all_but_params.
    """

    extension = "s3s"

    # ---------- Usage ----------

    @classmethod
    def usage(cls):
        usage = Usage(
            name="s3s",
            desc=(
                "S3 select source using <file>.s3s YAML config file.\n"
                "where <file>.s3s e.g:\n\n"
                "s3_bucket: my-bucket\n"
                "prefix: my-prefix\n"
                "sub_keys: # optional\n"
                "- 01\n"
                "- 02\n"
                "format: format.gz # csv, etc\n"
                "query: |\n"
                " SELECT s.FooCol FROM S3Object s\n"
                " WHERE s.IntCol = 42"
            ),
            component_class=cls,
        )
        usage.def_example(expr_tokens=["config.s3s", "-"], expect=None)
        usage.def_syntax(None)
        return usage

    # ---------- Construction ----------

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(root=None)

        config_path = ptok.all_but_params
        cfg = self._load_config(config_path)

        bucket = cfg.get("s3_bucket")
        prefix = cfg.get("prefix")
        fmt = cfg.get("format")
        query = cfg.get("query")

        if not bucket:
            raise ValueError("s3s config must include 's3_bucket'")
        if not prefix:
            raise ValueError("s3s config must include 'prefix'")
        if not fmt:
            raise ValueError("s3s config must include 'format'")
        if not query:
            raise ValueError("s3s config must include 'query'")

        key_regex = cfg.get("key_regex")

        prefixes = self._build_prefixes_from_config(prefix, cfg.get("sub_keys"))

        import boto3  # lazy
        s3 = boto3.client("s3")

        state = _SharedS3SelectState(
            s3_client=s3,
            bucket=bucket,
            prefixes=prefixes,
            format_override=fmt,
            query=query,
            key_regex=key_regex,
        )

        reserved = state.reserve_next_source()

        self._state = state
        self._current: Optional[Source] = reserved

    # Alternate ctor used by deep_copy
    @classmethod
    def _from_state(cls, state: _SharedS3SelectState, reserved: Optional[Source]):
        obj = cls.__new__(cls)
        Source.__init__(obj, root=None)
        obj._state = state
        obj._current = reserved
        return obj

    # ---------- Iteration / deep_copy ----------

    def __iter__(self):
        while True:
            if self._current is None:
                self._current = self._state.reserve_next_source()
                if self._current is None:
                    return  # exhausted

            for record in self._current:
                yield record

            self._current = None

    def deep_copy(self):
        reserved = self._state.reserve_next_source()
        if reserved is None:
            return None
        return self._from_state(self._state, reserved)

    # ---------- Config helpers ----------

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        if not isinstance(cfg, dict):
            raise ValueError("s3s config must be a YAML mapping at top level")
        return cfg

    @staticmethod
    def _build_prefixes_from_config(prefix: str, sub_keys: Optional[List[Any]]) -> List[str]:
        """
        If sub_keys present (list of suffix strings), produce prefix+suffix
        for each; otherwise just [prefix].
        """
        if not sub_keys:
            return [prefix]

        result: List[str] = []
        for s in sub_keys:
            # YAML might give ints or strings; normalize to str and strip
            suffix = str(s).strip()
            if not suffix:
                continue
            result.append(f"{prefix}{suffix}")
        return result
