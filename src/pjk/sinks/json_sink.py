# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from typing import IO
from .format_sink import FormatSink

class JsonSink(FormatSink):
    extension = 'json'

    def process(self) -> None:
        for record in self.input:
            self.outfile.write(json.dumps(record) + "\n")
        # Caller (DirSink/S3Sink) owns closing the outfile
