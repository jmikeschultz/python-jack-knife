# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz
#
# djk/pipes/postgres_pipe.py

import base64
import datetime as _dt
import uuid
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from pjk.usage import ParsedToken, Usage
from pjk.common import Integration
from pjk.pipes.query_pipe import QueryPipe

MAX_RETRIES = 3
BASE_DELAY = 0.1  # seconds

class DBClient:
    """Per-instance pg8000 connection wrapper. No shared state."""

    def __init__(
        self,
        host: str,
        username: str,
        password: Optional[str],
        db_name: str,
        port: int = 5432,
        ssl: bool = False,
    ):
        import pg8000  # lazy import

        kwargs = dict(
            user=username,
            password=password,
            host=host,
            database=db_name,
            port=port,
        )
        if ssl:
            import ssl as _ssl

            kwargs["ssl_context"] = _ssl.create_default_context()

        try:
            self.conn = pg8000.connect(**kwargs)
            self.conn.autocommit = True
        except Exception as e:
            print("Failed to connect to DB")
            raise e

    def close(self):
        if getattr(self, "conn", None) is None:
            return

        import pg8000  # lazy

        try:
            self.conn.close()
        except pg8000.exceptions.InterfaceError:
            # Already closed / broken; ignore.
            pass
        finally:
            self.conn = None


def _iso_dt(x: _dt.datetime) -> str:
    """ISO 8601; normalize UTC offset to 'Z'."""
    s = x.isoformat()
    return s.replace("+00:00", "Z")


def normalize(obj: Any) -> Any:
    """
    Make values JSON/YAML-safe and portable (schema-agnostic):
      - Decimal -> exact string (no sci-notation)
      - date/datetime/time -> ISO-8601 string (datetime keeps offset; UTC -> 'Z')
      - UUID -> string
      - bytes -> base64 string
      - lists/tuples/sets, dicts -> normalized recursively
      - leaves int/float/str/bool/None as-is
    """
    if obj is None:
        return None

    if isinstance(obj, Decimal):
        return format(obj, "f")  # exact value as string

    if isinstance(obj, _dt.datetime):
        return _iso_dt(obj)

    if isinstance(obj, (_dt.date, _dt.time)):
        return obj.isoformat()

    if isinstance(obj, uuid.UUID):
        return str(obj)

    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")

    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [normalize(v) for v in obj]

    return obj


def _row_to_dict(cursor, row) -> Dict[str, Any]:
    cols = [d[0] for d in cursor.description]
    return {col: normalize(val) for col, val in zip(cols, row)}


class PostgresPipe(QueryPipe, Integration):
    name = "postgres"
    desc = "Postgres query pipe; executes SQL over input record['query']."
    arg0 = ("instance", "instance of database.")
    examples = [
        ["myquery.sql", "postgres:mydb", "-"],
        ["{'query': 'SELECT * from MY_TABLE;'}", "postgres:mydb", "-"],
        ["{'query': 'SELECT * FROM pg_catalog.pg_tables;'}", "postgres:mydb"],
        ["{'query': 'SELECT procedure_batch(%s, ...), batch_params:{...}"],
        ["{'query': 'SELECT procedure_jsonb(%s, ...), json_params:json_string"],
    ]

    # name, type, default
    config_tuples = [
        ("db_name", str, None),
        ("host", str, None),
        ("user", str, None),
        ("password", str, None),
        ("port", int, 5432),
        ("ssl", bool, False),
    ]

    def __init__(self, ptok: ParsedToken, u: Usage, root=None):
        super().__init__(ptok, u, root=root)

        self.db_name = u.get_config("db_name")
        self.db_host = u.get_config("host")
        self.db_user = u.get_config("user")
        self.db_pass = u.get_config("password")
        self.db_port = u.get_config("port")
        self.db_ssl = u.get_config("ssl")

        # Standard params field: single-exec params (list/tuple/dict/single value)
        self.params_field = "params"

        # Legacy batch path: list[tuple|list|dict] → executemany
        self.batch_field = "batch_params"

        # Explicit JSON payload field (no query sniffing).
        # If present, this value is passed to cur.execute(query, json_params).
        self.json_params_field = "json_params"

        # One DB client (and thus one connection) per PostgresPipe instance.
        # Under your invariant (one thread per pipe), this is thread-safe.
        self.client = DBClient(
            host=self.db_host,
            username=self.db_user,
            password=self.db_pass,
            db_name=self.db_name,
            port=self.db_port,
            ssl=self.db_ssl,
        )

    def reset(self):
        # stateless across reset
        pass

    def close(self):
        if self.client is not None:
            self.client.close()

    def _make_header(self, cur, query: str, params=None) -> Dict[str, Any]:
        """
        Inspect the cursor and build a full header record.
        Figures out result, rowcount, function automatically.
        """
        h = {
            "db": self.db_name,
            "dbhost": self.db_host,
        }
        if params is not None:
            h["params"] = params

        if cur.description:
            cols = [d[0] for d in cur.description]
            if len(cols) == 1 and cols[0] == "ingest_event":
                _ = cur.fetchone()  # consume void row
                h["result"] = "ok"
                h["function"] = "ingest_event"
            else:
                h["result"] = "ok"
                h["rowcount"] = cur.rowcount if cur.rowcount != -1 else None
        else:
            h["result"] = "ok"
            h["rowcount"] = cur.rowcount

        return h

    def execute_query_returning_S_xO_iterable(self, record):
        query = record.get(self.query_field)
        if not query:
            record["_error"] = "missing query"
            yield record
            return

        # Priority: json_params > batch_params > params
        json_params = record.get(self.json_params_field, None)
        batch = record.get(self.batch_field, None)
        params = record.get(self.params_field, None)

        cur = self.client.conn.cursor()
        try:
            did_executemany = False
            header_params = None

            # ---------- execute ----------
            if json_params is not None:
                # Explicit JSON payload; caller controls shape.
                # We don't inspect query or payload.
                if isinstance(json_params, (list, tuple, dict)):
                    cur.execute(query, json_params)
                else:
                    cur.execute(query, (json_params,))
                header_params = {self.json_params_field: json_params}

            elif batch is not None:
                # Legacy executemany path; no magic.
                if len(batch) == 0:
                    cur.execute("SELECT 1")
                    header_params = {"batch_size": 0}
                elif len(batch) == 1:
                    cur.execute(query, batch[0])
                    header_params = {"batch_size": 1, "params": batch[0]}
                else:
                    cur.executemany(query, batch)
                    did_executemany = True
                    header_params = {"batch_size": len(batch)}

            else:
                # Single-statement path.
                if params is None:
                    cur.execute(query)
                    header_params = None
                else:
                    if isinstance(params, (list, tuple, dict)):
                        cur.execute(query, params)
                    else:
                        cur.execute(query, (params,))
                    header_params = params

            # ---------- header ----------
            yield self._make_header(cur, query, header_params)

            # ---------- stream rows (only meaningful for single execute that returns rows) ----------
            if not did_executemany and cur.description:
                cols = [d[0] for d in cur.description]
                if not (len(cols) == 1 and cols[0] == "ingest_event"):
                    for row in cur:
                        yield _row_to_dict(cur, row)

        finally:
            cur.close()
            # connection stays open for this pipe; closed in .close()
