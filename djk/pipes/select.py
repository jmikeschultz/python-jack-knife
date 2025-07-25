# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/select_pipe.py

from typing import Optional
from djk.base import Pipe, Usage, ParsedToken, UsageError

class SelectFields(Pipe):
    deep_copyable: bool = True

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        arg_string = ptok.get_arg(0)

        if not arg_string:
            raise UsageError("select:<f1,f2,...> requires at least one field")

        self.keep_fields = {f.strip() for f in arg_string.split(',') if f.strip()}
        if not self.keep_fields:
            raise UsageError("select must include at least one valid field name")


    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        
        for k in list(record.keys()):
            if k not in self.keep_fields:
                record.pop(k)

        return record
