# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.base import Sink

class MySink(Sink):
    def __init__(self, input_source, arg_string=""):
        super().__init__(input_source)
        self.max_copies = 7
        self.num_copied = 0
        self.instance_no = int(arg_string) if arg_string else 0        

    def process(self):
        while True:
            row = self.input.next()
            if row is None:
                break
            print(row)

    def deep_copy(self):
        if self.num_copied >= self.max_copies:
            return None

        self.num_copied += 1
        clone = self.input.deep_copy()
        return MySink(clone, str(self.num_copied))        
