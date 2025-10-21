# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz
#
# djk/pipes/snowflake_pipe.py

import base64
import datetime as _dt
import uuid
from decimal import Decimal
from typing import Any, Dict, Optional

from pjk.base import ParsedToken, TokenError, Usage, Integration
from pjk.pipes.query_pipe import QueryPipe
from pjk.common import Config


# ---------- utilities ----------

def _iso_dt(x: _dt.datetime) -> str:
    """ISO 8601; normalize UTC offset to 'Z' for UTC."""
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
        return format(obj, "f")
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


# ---------- client ----------

class SnowflakeClient:
    """
    Simple connection wrapper for snowflake-connector-python.
    One connection per client instance (safer than sharing across threads).
    """
    def __init__(
        self,
        *,
        account: str,
        user: str,
        password: Optional[str] = None,
        authenticator: Optional[str] = None,   # e.g. 'externalbrowser', 'oauth', 'snowflake'
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ):
        import snowflake.connector  # lazy import

        kwargs: Dict[str, Any] = {
            "account": account,
            "user": user,
        }
        if password:
            kwargs["password"] = password
        if authenticator:
            kwargs["authenticator"] = authenticator
        if role:
            kwargs["role"] = role
        if warehouse:
            kwargs["warehouse"] = warehouse
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema

        try:
            self.conn = snowflake.connector.connect(**kwargs)
            # autocommit is True by default; make explicit
            self.conn.autocommit(True)
            # Apply explicit USE statements as a safety net (only if provided)
            with self.conn.cursor() as cur:
                if role:
                    cur.execute(f'USE ROLE "{role}"')
                if warehouse:
                    cur.execute(f'USE WAREHOUSE "{warehouse}"')
                if database:
                    cur.execute(f'USE DATABASE "{database}"')
                if schema:
                    cur.execute(f'USE SCHEMA "{schema}"')
        except Exception as e:
            print("Failed to connect to Snowflake")
            raise e

    def close(self):
        if getattr(self, "conn", None) is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None


# ---------- pipe ----------

class SnowflakePipe(QueryPipe, Integration):
    """
    Snowflake query pipe; executes SQL found in input record['query'] and streams rows.
    Connection/session settings are pulled from ~/.pjk/component_configs.yaml under the arg name.
    """
    name = 'snowflake'
    desc = "Snowflake query pipe; executes an SQL query for each input record."
    arg0 = ('dbname', 'database name.')
    examples = [
        ["{'query': 'SELECT CURRENT_ROLE();'}", "snowflake:EDLDB", "-"],
        ["myquery.sql", "snowflake:EDLDB", "-"]
    ]

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        self.dbname = usage.get_arg('dbname')
        config = Config('dbname', self, self.dbname)
        self.sf_account  = config.lookup("account")
        self.sf_user     = config.lookup("user")
        self.sf_auth     = config.lookup("authenticator")
        self.sf_role     = config.lookup("role")
        self.sf_wh       = config.lookup("warehouse")
        self.sf_schema   = config.lookup("schema")
        self.sf_db       = self.dbname

        # Basic validation
        missing = [k for k, v in [
            ("account", self.sf_account),
            ("user", self.sf_user),
            ("authenticator|password", self.sf_auth or self.sf_password),
            ("role", self.sf_role),
            ("warehouse", self.sf_wh),
            ("schema", self.sf_schema),
        ] if not v]
        if missing:
            raise TokenError(
                f"config entry '{self.dbname}' missing: {', '.join(missing)}"
            )

        self.query_field = usage.get_param('query_field')
        self.params_field = "params"  # optional: list/tuple (positional) or dict (named)

    def reset(self):
        # stateless across reset
        pass

    def _make_header(self, cur, params=None) -> Dict[str, Any]:
        """
        Build a header record with query metadata and session context.
        """
        h: Dict[str, Any] = {
            "db": self.dbname,
            "account": self.sf_account,
            "role": self.sf_role,
            "warehouse": self.sf_wh,
        }
        if self.sf_db:
            h["database"] = self.sf_db
        if self.sf_schema:
            h["schema"] = self.sf_schema
        if params is not None:
            h["params"] = params

        # Snowflake's cursor.rowcount is often -1 for SELECT until fully fetched.
        # We still include it if known (for DML it may be accurate).
        try:
            rc = getattr(cur, "rowcount", None)
            if isinstance(rc, int) and rc >= 0:
                h["rowcount"] = rc
        except Exception:
            pass

        h["result"] = "ok"
        return h

    def execute_query_returning_Q_xR_iterable(self, record):
        client = SnowflakeClient(
            account=self.sf_account,
            user=self.sf_user,
            authenticator=self.sf_auth,
            role=self.sf_role,
            warehouse=self.sf_wh,
            database=self.sf_db,
            schema=self.sf_schema,
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
                    # Execute (supports positional or named params per DB-API)
                    if params is None:
                        cur.execute(query)
                    else:
                        if isinstance(params, (list, tuple, dict)):
                            cur.execute(query, params)
                        else:
                            # single scalar -> positional 1-tuple
                            cur.execute(query, (params,))

                    yield self._make_header(cur, params)

                    # Stream result rows for queries that return a result set
                    if cur.description:
                        for row in cur:
                            yield _row_to_dict(cur, row)
                finally:
                    cur.close()
        finally:
            client.close()
