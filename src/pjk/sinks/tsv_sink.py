# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.sinks.csv_sink import CSVSink
from pjk.base import Usage
from typing import IO

class TSVSink(CSVSink):
    extension = 'tsv'

    def __init__(self, outfile: IO[str]):
        super().__init__(outfile, delimiter="\t")
