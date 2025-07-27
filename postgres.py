# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import sys
from typing import Optional
from psycopg import Connection
from aws_advanced_python_wrapper import AwsWrapperConnection
from djk.base import Source

def status(msg):
    print(f"[postgres] {msg}", file=sys.stderr)

class PostgresSource(Source):
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conn: Optional[Connection] = None
        self.cursor = None
        self.columns = None
        status(f"Preparing to read from table '{table_name}'")

    def next(self) -> Optional[dict]:
        if self.cursor is None:
            self._init_cursor()

        row = self.cursor.fetchone()
        if row is None:
            status(f"End of data from '{self.table_name}'")
            self._close()
            return None

        return dict(zip(self.columns, row))

    def _init_cursor(self):
        creds_path = os.path.join(os.getcwd(), "postgres.txt")
        status(f"Reading credentials from {creds_path}")

        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Missing credentials file: {creds_path}")

        with open(creds_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]
            creds = dict(line.split("=", 1) for line in lines)

        required = {"user", "pass", "host", "port", "dbname"}
        missing = required - creds.keys()
        if missing:
            raise RuntimeError(f"Missing fields in postgres.txt: {', '.join(missing)}")

        user = creds["user"]
        password = creds["pass"]
        host = creds["host"]
        port = int(creds["port"])
        dbname = creds["dbname"]

        status(f"Connecting to '{dbname}' at {host}:{port} as '{user}' using AWS wrapper...")

        try:
            self.conn = AwsWrapperConnection.connect(
                Connection.connect,
                f"host={host} dbname={dbname} user={user} password={password} port={port} sslmode=require",
                plugins="failover",
                wrapper_dialect="aurora-pg",
                connect_timeout=5,
                autocommit=True
            )
            self.cursor = self.conn.cursor()
            status("Connected.")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to PostgreSQL with AWS wrapper: {e}")

        try:
            self.cursor.execute(f"SELECT * FROM {self.table_name}")
            self.columns = [desc.name for desc in self.cursor.description]
            status(f"Loaded {len(self.columns)} columns from table '{self.table_name}'")
        except Exception as e:
            self._close()
            raise RuntimeError(f"Query failed: {e}")

    def _close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
