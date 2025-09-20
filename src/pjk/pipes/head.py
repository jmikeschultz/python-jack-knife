# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/head.py

from typing import Optional
from pjk.base import Pipe, ParsedToken, Usage, DeepCopyPipe

class HeadPipe(DeepCopyPipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='head',
            desc='take first records of input (when single-threaded)',
            component_class=cls
        )
        usage.def_arg(name='limit', usage='number of records', is_num=True)
        usage.def_example(expr_tokens=['[{id:1}, {id:2}]', 'head:1'], expect="{id:1}")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.limit = usage.get_arg('limit')
        self.count = 0

    def __iter__(self):
        for record in self.left:
            if self.count >= self.limit:
                break
            self.count += 1
            yield record
    
    def reset(self):
        self.count = 0
