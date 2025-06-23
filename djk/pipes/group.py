# djk/pipes/group.py

from typing import Optional
from djk.base import Pipe
from datetime import datetime

''' exhausts source to group them by argument fields'''
class GroupPipe(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)
        self.field = arg_string
        self.fields = arg_string.split(',')
        self.out_recs = None

    def load(self):
        recs = {}
        while True:
            record = self.inputs[0].next()
            if record is None:
                break
            key_rec = {}
            for field in self.fields:
                key_rec[field] = record.pop(field, None)

            key = tuple(key_rec.values())

            lookup = recs.get(key, None)
            if not lookup:
                key_rec['child'] = []
                key_rec['child'].append(record)
                recs[key] = key_rec
            else:
                entries = lookup.get('child')
                entries.append(record)

        return list(recs.values())

    def next(self) -> Optional[dict]:
        if self.out_recs == None:
            self.out_recs = self.load()

        if len(self.out_recs) == 0:
            return None

        return self.out_recs.pop()
            
