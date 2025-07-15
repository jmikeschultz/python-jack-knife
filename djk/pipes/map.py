# djk/pipes/map.py

from typing import Optional
from djk.base import Pipe, KeyedSource, ParsedToken, Usage

class MapPipe(Pipe, KeyedSource):
    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)

        self.field = ptok.get_arg(0)
        self.map = {}
        self.keys = []
        self.index = 0
        self.loaded = False

    def _load_all(self):
        source = self.inputs[0]
        while True:
            record = source.next()
            if record is None:
                break
            key = record.get(self.field)
            if key is not None:
                self.map[key] = record
        self.keys = list(self.map)
        self.loaded = True

    def next(self) -> Optional[dict]:
        if not self.loaded:
            self._load_all()
        if self.index >= len(self.keys):
            return None
        key = self.keys[self.index]
        self.index += 1
        return self.map[key]

    def get_keyed_field(self) -> str:
        return self.field

    def get_record(self, key) -> Optional[dict]:
        if not self.loaded:
            self._load_all()
        return self.map.get(key)
