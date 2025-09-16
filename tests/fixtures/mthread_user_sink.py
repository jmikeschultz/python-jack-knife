# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Sink, ParsedToken, Usage

class MySink(Sink):
    def __init__(self, ptok: ParsedToken, usage: Usage, instance_no: int):
        super().__init__(ptok, usage))
        self.max_copies = 7
        self.num_copied = 0
        self.instance_no = 0

    def process(self):
        while True:
            for row in self.input:
                print(row)

    def deep_copy(self):
        if self.num_copied >= self.max_copies:
            return None

        self.num_copied += 1
        clone = self.input.deep_copy()
        return MySink(clone, self.ptok, self.usage, str(self.num_copied))
