# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/denorm.py

from pjk.base import Pipe, ParsedToken, Usage, UsageError
from typing import Iterator

class Denormer:
    def __init__(self, record, field):
        self.field = field
        data = record.pop(field, None)

        if not data:
            self.subrec_list = [record]
            self.base_record = {}
            return

        self.base_record = record

        if isinstance(data, list):
            self.subrec_list = data
        elif isinstance(data, dict):
            self.subrec_list = [data]
        else:
            raise UsageError("can only denorm sub-records")

    def __iter__(self) -> Iterator[dict]:
        for subrec in self.subrec_list:
            if not isinstance(subrec, dict):
                subrec = {self.field: subrec}
            out = self.base_record.copy()
            out.update(subrec)
            yield out


class DenormPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='explode',
            desc='Explode a nested list/dict field into separate flattened records',
            component_class=cls
        )
        usage.def_arg(name='field', usage='Field to explode')
        usage.def_example(expr_tokens=["{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}",
                                       'explode:cars'
                                       ],
                        expect="[{ferry:'orca', make: 'ford', size:9}, {ferry:'orca', make:'bmw', size:4}]")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        self.field = usage.get_arg('field')
        if not self.field:
            raise UsageError("denorm must include a field name")

        self._pending_iter = None

    def reset(self):
        self._pending_iter = None

    def __iter__(self):
        for record in self.left:
            denormer = Denormer(record, self.field)
            for out in denormer:
                yield out
