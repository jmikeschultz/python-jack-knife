# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import csv
from typing import IO, Dict, Any
from .format_sink import FormatSink

class CSVSink(FormatSink):
    extension = "csv"

    def __init__(self, outfile: IO[str], delimiter:str = ','):
        super().__init__(outfile=outfile)
        self.delimiter = delimiter

    def process(self) -> None:
        writer = None
        for record in self.input:
            if writer is None:
                # Initialize DictWriter with dynamic fieldnames from first record
                writer = csv.DictWriter(self.outfile, fieldnames=record.keys(), delimiter=self.delimiter)
                writer.writeheader()
            writer.writerow(record)
