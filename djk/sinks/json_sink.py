# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import gzip
import json
from djk.base import Sink, Source, ParsedToken, Usage

class JsonSink(Sink):
    is_format = True

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.path_no_ext = ptok.pre_colon # NOTE: ptok built by framework, doesn't use usage
        self.gz = ptok.get_arg(0) == 'True'# NOTE: ptok built by framework, doesn't use usage

    def process(self) -> None:
        path = self.path_no_ext + ('.json.gz' if self.gz else '.json')
        open_func = gzip.open if self.gz else open

        with open_func(path, 'wt', encoding='utf-8') as f:
            for record in self.input:
                f.write(json.dumps(record) + '\n')
