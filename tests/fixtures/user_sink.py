# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Sink, Source, ParsedToken, Usage

class MySink(Sink):
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage))

    def process(self):
        while True:
            for row = self.input:
                print(row)
