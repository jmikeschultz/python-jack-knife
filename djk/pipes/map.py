# djk/pipes/group.py

from typing import Optional
from djk.base import ParsedToken, Usage, Pipe, KeyedSource
from datetime import datetime

''' exhausts source to group them by argument fields'''
class MapPipe(Pipe, KeyedSource):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='map',
            desc="keyed source map records either overriding or grouping duplicates."
        )
        usage.def_arg(name='how', usage="'o' for override, 'g' for group")
        usage.def_arg(name='key', usage='comma separated fields to map by')
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.is_group = usage.get_arg('how') == 'g' # or 'o' for override
        self.fields = usage.get_arg('key').split(',')
        self.rec_map = {}
        self.matched_map = {} # for records returned by get_record (for join:right)
        self.rec_list = None
        self.is_loaded = False

    def load(self):
        self.is_loaded = True
        while True:
            record = self.inputs[0].next()
            if record is None:
                break
            key_rec = {}
            for field in self.fields:
                key_rec[field] = record.pop(field, None)

            key = tuple(key_rec.values())

            lookup = self.rec_map.get(key, None)
            if not lookup:
                if self.is_group:
                    key_rec['child'] = []
                    key_rec['child'].append(record)
                    self.rec_map[key] = key_rec
                else:
                    self.rec_map[key] = record
            else:
                if self.is_group:
                    entries = lookup.get('child')
                    entries.append(record)
                else:
                    self.rec_map[key] = record

    def next(self) -> Optional[dict]:
        if not self.rec_list == None:
            if not self.is_loaded:
                self.load()
            self.rec_list = list(self.rec_map.values())

        if len(self.rec_list) == 0:
            return None

        return self.rec_list.pop()
            
    def lookup(self, left_rec) -> Optional[dict]:
        if not self.is_loaded:
            self.load()

        key = tuple(left_rec.get(f) for f in self.fields)
        rec = self.rec_map.pop(key, None)

        if rec:
            self.matched_map[key] = rec
            return rec
        else:
            rec = self.matched_map.get(key) # may have been moved
            return rec

    def get_unlookedup_records(self):
        if not self.is_loaded:
            self.load()

        return list(self.rec_map.values())
