# djk/sources/inline_source
import hjson
import gzip
from typing import Optional
from djk.base import Source, Report
from collections import OrderedDict

def to_builtin(obj):
    """Recursively convert OrderedDicts to dicts and lists."""
    if isinstance(obj, OrderedDict):
        return {k: to_builtin(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_builtin(v) for v in obj]
    else:
        return obj

class InlineSource(Source):
    def __init__(self, inline_expr):
        self.file = None
        self.num_recs = 0

        # hjson doesn't require strings be quoted
        obj = hjson.loads(inline_expr)

        if isinstance(obj, dict):
            self.records = [obj]
        elif isinstance(obj, list):
            self.records = obj

    @classmethod
    def is_inline(cls, token):
        if len(token) < 2:
            return False

        if token[0] == '{' and token[-1] == '}':
            return True
        
        if token[0] == '[' and token [-1] == ']':
            return True
        
        return False

    def next(self) -> Optional[dict]:
        if len(self.records) == 0:
            return None

        parsed = self.records.pop(0)
        native = to_builtin(parsed)

        return native
