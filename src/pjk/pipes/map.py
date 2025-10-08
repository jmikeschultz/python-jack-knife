# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/group.py

from typing import Optional
from pjk.base import ParsedToken, Usage, Pipe, KeyedSource

class MapByPipe(Pipe, KeyedSource):
    @classmethod
    def usage(cls):
        u = Usage(
            name='mapby',
            desc="Maps records to key, taking last instance of duplicates.\nFilters out records without all key fields.\nCreates Keyed Source for join or filter.",
            component_class=cls
        )
        u.def_arg(name='key', usage='comma separated fields to map by')
        u.def_param(name='count', usage='add count of the records with key', valid_values={'true', 'false'}, default='false')
        u.def_example(expr_tokens=["[{id: 1, color:'blue'}, {id:1, color:'green'}, {id:2, color:'red'}]", 'mapby:id'],
                          expect="[{id:2, color:'red'}, {id:1, color:'green'}]")
        u.def_example(expr_tokens=["[{id: 1, color:'blue', size:5}, {id:1, color:'green', size:10}]", 'mapby:id,color'], 
                          expect="[{id:1, color:'green', size: 10}, {id:1, color:'blue', size:5}]")
        u.def_example(expr_tokens=["[{id:'a'}, {id:'a'}, {id:'b'}, {j:3}]", "mapby:id@count=true"],
                        expect="[{id:'a', count:2}, {id:'b', 'count': 1}]")

        return u

    def __init__(self, ptok: ParsedToken, usage: Usage, is_group: bool = False):
        super().__init__(ptok)
        self.is_group = is_group
        self.fields = usage.get_arg('key').split(',')
        self.rec_map = {}
        self.matched_map = {}
        self.is_loaded = False
        self.do_count = usage.get_param(name='count').lower() == 'true'
        self.counts = {}

    def reset(self):
        self.rec_map.clear()
        self.matched_map.clear()
        self._rec_list = None
        self.is_loaded = False

    def get_key_rec(self, record):
        key_rec = {}
        for field in self.fields:
            key_val = record.pop(field, None) if self.is_group else record.get(field)
            if not key_val:
                return None
            
            key_rec[field] = key_val
        return key_rec
    
    def count(self, key):
        if not self.do_count:
            return
        i = self.counts.get(key, 0)
        self.counts[key] = i+1

    def load(self):
        if self.is_loaded:
            return
        self.is_loaded = True

        for record in self.left:
            key_rec = self.get_key_rec(record)
            if not key_rec: # some fields missing, filter out rec
                continue

            key = tuple(key_rec.values())
            self.count(key)

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

        if self.do_count:
            for k, v in self.rec_map.items():
                if self.do_count:
                    c = self.counts.get(k, 0)
                    v['count'] = c

    def __iter__(self):
        if not self.is_loaded:
            self.load()
        for v in self.rec_map.values():
            yield v

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
        u = Usage(
            name='groupby',
            desc="groups records by key. Creates Keyed Source for join or filter.",
            component_class=cls
        )
        u.def_arg(name='key', usage='comma separated fields to map by')
        u.def_param(name='count', usage='add count of the records with key', valid_values={'true', 'false'}, default='false')
        u.def_example(expr_tokens=["[{id: 1, color:'blue'}, {id:1, color:'green'}, {id:2, color:'red'}]", 'groupby:id'], 
                          expect="[{id:2, child:[{color:'red'}]}, {id:1, child:[{color:'blue'},{color: 'green'}]}]")

        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage, True)