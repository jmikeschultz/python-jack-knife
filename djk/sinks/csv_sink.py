import csv
from djk.base import Sink, Source

class CSVSink(Sink):
    def __init__(self, input_source: Source, path_no_ext: str, delimiter: str = ",", ext: str = 'csv'):
        super().__init__(input_source)
        self.path = f'{path_no_ext}.{ext}'
        self.delimiter = delimiter

    def process(self) -> None:
        with open(self.path, 'w', newline='') as f:
            writer = None

            while True:
                record = self.input.next()
                if record is None:
                    break

                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=record.keys(), delimiter=self.delimiter)
                    writer.writeheader()

                writer.writerow(record)
