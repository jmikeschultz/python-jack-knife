# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/group.py

from typing import Optional
from pjk.base import ParsedToken, Usage, Pipe, KeyedSource

class MapByPipe(Pipe, KeyedSource):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='mapby',
            desc="maps records to key, taking last instance of duplicates.\nCreates Keyed Source for join or filter.",
            component_class=cls
        )
        usage.def_arg(name='key', usage='comma separated fields to map by')
        usage.def_example(expr_tokens=["[{id: 1, color:'blue'}, {id:1, color:'green'}, {id:2, color:'red'}]", 'mapby:id'],
                          expect="[{id:2, color:'red'}, {id:1, color:'green'}]")
        usage.def_example(expr_tokens=["[{id: 1, color:'blue', size:5}, {id:1, color:'green', size:10}]", 'mapby:id,color'], 
                          expect="[{id:1, color:'green', size: 10}, {id:1, color:'blue', size:5}]")

        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage, is_group: bool = False):
        super().__init__(ptok)
        self.is_group = is_group
        self.fields = usage.get_arg('key').split(',')
        self.rec_map = {}
        self.matched_map = {}
        self._rec_list = None
        self.is_loaded = False

    def reset(self):
        self.rec_map.clear()
        self.matched_map.clear()
        self._rec_list = None
        self.is_loaded = False

    def load(self):
        if self.is_loaded:
            return
        self.is_loaded = True

        for record in self.left:
            key_rec = {}
            for field in self.fields:
                key_rec[field] = record.pop(field, None) if self.is_group else record.get(field)

            key = tuple(key_rec.values())
            existing = self.rec_map.get(key)

            if not existing:
                if self.is_group:
                    key_rec['child'] = [record]
                    self.rec_map[key] = key_rec
                else:
                    self.rec_map[key] = record
            else:
                if self.is_group:
                    existing['child'].append(record)
                else:
                    self.rec_map[key] = record

    def __iter__(self):
        if not self.is_loaded:
            self.load()
        if self._rec_list is None:
            self._rec_list = list(self.rec_map.values())

        while self._rec_list:
            yield self._rec_list.pop()

    def lookup(self, left_rec) -> Optional[dict]:
        if not self.is_loaded:
            self.load()

        key = tuple(left_rec.get(f) for f in self.fields)
        rec = self.rec_map.pop(key, None)
        if rec is not None:
            self.matched_map[key] = rec
            return rec
        return self.matched_map.get(key)

    def get_unlookedup_records(self):
        if not self.is_loaded:
            self.load()
        return list(self.rec_map.values())

class GroupByPipe(MapByPipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='groupby',
            desc="groups records by key. Creates Keyed Source for join or filter.",
            component_class=cls
        )
        usage.def_arg(name='key', usage='comma separated fields to map by')
        usage.def_example(expr_tokens=["[{id: 1, color:'blue'}, {id:1, color:'green'}, {id:2, color:'red'}]", 'groupby:id'], 
                          expect="[{id:2, child:[{color:'red'}]}, {id:1, child:[{color:'blue'},{color: 'green'}]}]")

        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage, is_group: bool = False):
        super().__init__(ptok, usage, True)