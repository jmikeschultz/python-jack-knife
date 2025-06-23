# djk/pipes/denorm.py

from typing import Optional
from djk.base import Pipe, Source, PipeSyntaxError

class Denormer:
    def __init__(self, record, field):
        data = record.pop(field, None)
        if not data:
            self.subrec_list = [record]
            self.base_record = {}
            return

        self.base_record = record
        if isinstance(data, list):
            self.subrec_list = data

        elif isinstance(data, dict):
            subrec_list = [data]

        else:
            raise PipeSyntaxError("can only denorm sub-records")

    def next(self):
        if len(self.subrec_list) == 0:
            return None
        subrec = self.subrec_list.pop()
        out = self.base_record.copy() # deep copy?
        out.update(subrec)
        return out

class DenormPipe(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)

        self.field = arg_string # field to denorm
        if not self.field:
            raise PipeSyntaxError("select must include at least one valid field name")

        self.denormer = None

    def next(self) -> Optional[dict]:
        if not self.denormer:
            record = self.inputs[0].next()
            if record is None:
                return None
            self.denormer = Denormer(record, self.field)

        rec = self.denormer.next()
        if rec:
            return rec
        else:
            self.denormer = None
            return self.next()

