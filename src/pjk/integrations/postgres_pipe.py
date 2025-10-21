# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz
#
# djk/pipes/postgres_pipe.py

import base64
import datetime as _dt
import uuid
from decimal import Decimal
from typing import Any, Dict, Optional

from pjk.base import Integration, ParsedToken, Usage
from pjk.common import Config
from pjk.pipes.query_pipe import QueryPipe


class DBClient:
    """Simple shared-connection wrapper for pg8000."""
    _connection = None
    
    def __init__(self, host: str, username: str, password: Optional[str],
                 dbname: str, port: int = 5432, ssl: bool = False):
        import pg8000 # lazy import
        if DBClient._connection is None:
            try:
                kwargs = dict(user=username, password=password, host=host, database=dbname, port=port)
                if ssl:
                    import ssl as _ssl
                    kwargs["ssl_context"] = _ssl.create_default_context()
                DBClient._connection = pg8000.connect(**kwargs)
                DBClient._connection.autocommit = True
            except Exception as e:
                print("Failed to connect to DB")
                raise e
        self.conn = DBClient._connection

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                DBClient._connection = None


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


class PostgresPipe(QueryPipe,Integration):
    name = 'postgres'
    desc = "Postgres query pipe; executes SQL from input."
    arg0 = ("dbname", 'database name.')
    examples = [
        ['myquery.sql', 'postgres:mydb', '-'],
        ["{'query': 'SELECT * from MY_TABLE;'}", 'postgres:mydb', '-'],
        ["{'query': 'SELECT * FROM pg_catalog.pg_tables;'}", 'postgres:mydb']
    ]

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        self.dbname = usage.get_arg("dbname")
        config = Config('dbname', self, self.dbname)
        self.db_host = config.lookup("host")
        self.db_user = config.lookup("user")
        self.db_pass = config.lookup("password")
        self.db_port = int(config.lookup("port", 5432))
        self.db_ssl  = bool(config.lookup("ssl", False))

        self.query_field  = usage.get_param('query_field')
        self.params_field = "params"  # optional: list/tuple (positional) or dict (named)

    def reset(self):
        # stateless across reset
        pass

    def _make_header(self, cur, query: str, params=None) -> Dict[str, Any]:
        """
        Inspect the cursor and build a full header record.
        Figures out result, rowcount, function automatically.
        """
        h = {
            "db": self.dbname,
            "dbhost": self.db_host,
        }
        if params:
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

    def execute_query_returning_Q_xR_iterable(self, record):
        client = DBClient(
            host=self.db_host,
            username=self.db_user,
            password=self.db_pass,
            dbname=self.dbname,
            port=self.db_port,
            ssl=self.db_ssl,
        )
        try:
            query = record.get(self.query_field)
            if not query:
                    record['_error'] = 'missing query'
                    yield record
            else:        
                params = record.get(self.params_field)

                cur = client.conn.cursor()
                try:
                    # execute
                    if params is None:
                        cur.execute(query)
                    else:
                        if isinstance(params, (list, tuple, dict)):
                            cur.execute(query, params)
                        else:
                            cur.execute(query, (params,))

                    # yield header first
                    yield self._make_header(cur, query, params)

                    # then stream rows if it was a real SELECT with results
                    if cur.description:
                        cols = [d[0] for d in cur.description]
                        if not (len(cols) == 1 and cols[0] == "ingest_event"):
                            for row in cur:
                                yield _row_to_dict(cur, row)
                finally:
                    cur.close()
        finally:
            client.close()
