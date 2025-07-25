# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/remove_field.py

from typing import Optional
from djk.base import Pipe, ParsedToken, Usage, UsageError

class RemoveField(Pipe):
    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)

        arg_string = ptok.get_arg(0)

        self.fields = [f.strip() for f in arg_string.split(',') if f.strip()]
        if not self.fields:
            raise UsageError("rm must include at least one valid field name")
        self.count = 0

    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if not record:
            return None

        self.count += 1
        for field in self.fields:
            record.pop(field, None)
            
        return record
