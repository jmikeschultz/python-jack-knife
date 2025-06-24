from djk.base import Sink, Source, PipeSyntaxError
import boto3
from decimal import Decimal

class DDBSink(Sink):
    def __init__(self, input_source: Source, arg_str: str):
        super().__init__(input_source)
        self.batch = []
        self.batch_size = 10  # You can increase this up to 25
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(arg_str)
        self.num_recs = 0

    def process_batch(self):
        if not self.batch:
            return
        
        with self.table.batch_writer() as batch:
            for item in self.batch:
                # Ensure Decimal compliance for numeric values
                clean_item = {
                    k: (Decimal(str(v)) if isinstance(v, float) else v)
                    for k, v in item.items()
                }
                batch.put_item(Item=clean_item)
        self.batch = []

    def process(self):
        while True:
            record = self.input.next()
            if record is None:
                break

            self.batch.append(record)
            self.num_recs += 1
            if len(self.batch) >= self.batch_size:
                self.process_batch()

        # Process any remaining records
        self.process_batch()
        print(self.num_recs)
