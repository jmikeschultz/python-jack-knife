# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/postgres_pipe.py

import os
import pg8000
import uuid
import datetime
from djk.base import Pipe, ParsedToken, NoBindUsage, Usage, TokenError

class DBClient:
    _connection = None

    def __init__(self, host, username, password, dbname='postgres', port=5432):
        if DBClient._connection is None:
            try:
                DBClient._connection = pg8000.connect(
                    user=username,
                    password=password,
                    host=host,
                    database=dbname,
                    port=port
                )
            except Exception as e:
                print("Failed to connect to DB")
                raise e
        self.conn = DBClient._connection

    def query(self, sql, params=None):
        cursor = self.conn.cursor()
        cursor.execute(sql, params or ())
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(colnames, normalize(row))) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            DBClient._connection = None

def normalize(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, tuple):
        return [normalize(x) for x in obj]
    elif isinstance(obj, list):
        return [normalize(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items()}
    return obj

class PostgresPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='postgres',
            desc="Postgres query pipe; reads SQL from record['query'] and DB name from record['db'] (default: postgres)",
            component_class=cls
        )
        usage.def_param('no_header', usage='omit header record before query result', is_num=False, valid_values={'true', 'false'})
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        self.query_field = 'query'
        self.db_field = 'db'
        self.db_host = os.environ.get("PG_HOST")
        self.db_user = os.environ.get("PG_USER")
        self.db_pass = os.environ.get("PG_PASS")
        self.no_header = usage.get_param('no_header', default='false') == 'true'

        if not all([self.db_host, self.db_user, self.db_pass]):
            raise TokenError("PG_HOST, PG_USER, and PG_PASS must be set in environment")

    def reset(self):
        pass  # stateless across reset

    def __iter__(self):
        for input_record in self.left:
            query = input_record.get(self.query_field)
            dbname = input_record.get(self.db_field, 'postgres')

            if not query:
                yield {'_error': 'missing query'}
                continue

            client = DBClient(
                host=self.db_host,
                username=self.db_user,
                password=self.db_pass,
                dbname=dbname
            )

            try:
                cursor = client.conn.cursor()
                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cursor.close()

                if not self.no_header:
                    yield {'header': {'query': query, 'db': dbname}}

                for row in rows:
                    yield {k: normalize(v) for k, v in zip(colnames, row)}

            finally:
                client.close()
