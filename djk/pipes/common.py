from djk.base import Source, Pipe, UsageError

class SafeNamespace:
    def __init__(self, obj):
        for k, v in obj.items():
            if isinstance(v, dict):
                v = SafeNamespace(v)
            elif isinstance(v, list):
                v = [SafeNamespace(x) if isinstance(x, dict) else x for x in v]
            setattr(self, k, v)

    def __getattr__(self, key):
        return None  # gracefully handle missing keys

class ReducingNamespace:
    def __init__(self, record):
        self._record = record

    def __getattr__(self, name):
        value = self._record[name]
        if isinstance(value, (list, tuple, set)):
            return value
        return [value]  # promote scalars to singleton lists
