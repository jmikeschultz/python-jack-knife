# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import pg8000
from djk.base import Pipe, ParsedToken, NoBindUsage, Usage, TokenError
import uuid
import datetime

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
        colnames = [desc[0] for desc in cursor.description]  #!! get column names
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(colnames, normalize(row))) for row in rows]  #!! return list of dicts

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
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.query_field = 'query'
        self.db_field = 'db'

        self.db_host = os.environ.get("PG_HOST")
        self.db_user = os.environ.get("PG_USER")
        self.db_pass = os.environ.get("PG_PASS")

        self.no_header = usage.get_param('no_header', default="false") in 'true'

        if not all([self.db_host, self.db_user, self.db_pass]):
            raise TokenError("PG_HOST, PG_USER, and PG_PASS must be set in environment")

        self.buffer = None  #!! emit stream record-by-record

    def next(self):
        #!! Emit buffered rows one at a time
        if self.buffer:
            return self.buffer.pop(0)

        input_record = self.inputs[0].next()
        if input_record is None:
            return None

        query = input_record.get(self.query_field)
        dbname = input_record.get(self.db_field, 'postgres')

        if not query:
            return dict(_error='missing query')

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

            def normalize(val):
                import uuid, datetime
                if isinstance(val, uuid.UUID):
                    return str(val)
                elif isinstance(val, datetime.datetime):
                    return val.isoformat()
                elif isinstance(val, datetime.date):
                    return val.isoformat()
                return val

            #!! buffer first record as header, rest as row dicts
            self.buffer = [] if self.no_header else [{'header': {'query': query, 'db': dbname}}] 
            for row in rows:
                self.buffer.append({k: normalize(v) for k, v in zip(colnames, row)})

            return self.buffer.pop(0)

        finally:
            client.close()

    @classmethod
    def usage(cls):
        usage = Usage(
            name='postgres',
            desc="Postgres query pipe, reads queries from input source"
        )

        usage.def_param('no_header', usage='omit the header and only stream query results', is_num=False, valid_values={'true', 'false'})
        return usage
