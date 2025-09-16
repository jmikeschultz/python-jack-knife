# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Sink, Source, ParsedToken, Usage
from decimal import Decimal

class DDBSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='ddb',
            desc='Write records to a DynamoDB table via batch_writer()',
            component_class=cls
        )
        usage.def_arg('table', usage='DynamoDB table name')
        usage.def_param('batch_size', usage='How many records to write per batch (max 25)')
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        import boto3 # lazy import

        self.table_name = usage.get_arg('table')
        self.batch_size = int(usage.get_param('batch_size', default='10'))
        self.num_recs = 0
        self.batch = []

        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(self.table_name)

    def process_batch(self):
        if not self.batch:
            return

        with self.table.batch_writer() as batch:
            for item in self.batch:
                clean_item = {
                    k: (Decimal(str(v)) if isinstance(v, float) else v)
                    for k, v in item.items()
                }
                batch.put_item(Item=clean_item)

        self.batch = []

    def process(self):
        for record in self.input:
            self.batch.append(record)
            self.num_recs += 1

            if len(self.batch) >= self.batch_size:
                self.process_batch()

        self.process_batch()
        print(f"DDBSink wrote {self.num_recs} records.")
