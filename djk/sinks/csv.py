import sys
import csv
import json
from djk.base import Sink, Source

class CSVSink(Sink):
    def __init__(self, input_source: Source, path: str):
        super().__init__(input_source)
        self.path = path

    def process(self) -> None:
        with open(self.path, 'w', newline='') as f:
            writer = None

            while True:
                record = self.input.next()
                if record is None:
                    break

                if writer is None:
                    # Initialize writer with fieldnames from the first record
                    writer = csv.DictWriter(f, fieldnames=record.keys())
                    writer.writeheader()

                writer.writerow(record)
