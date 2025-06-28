import os
import queue
from djk.base import Source, Pipe, SyntaxError
from djk.sources.json_source import JsonSource
from djk.sources.csv_source import CSVSource
from djk.sources.s3_source import S3Source
from djk.sources.source_list import SourceListSource
from djk.sources.inline_source import InlineSource
from djk.sources.dir_source import DirSource

from djk.sources.lazy_file import LazyFile
from djk.sources.lazy_file_local import LazyFileLocal

class SourceFactory:

    @classmethod
    def source_class_getter(cls, name, parms) -> Source:
        if name.endswith(".json") or name.endswith(".json.gz") or 'format=json' in parms:
            return JsonSource
        if name.endswith(".csv") or name.endswith(".csv.gz") or 'format=csv' in parms:
            return CSVSource
            
        return None

    @classmethod
    def create(cls, token) -> Source:
        token = token.strip()

        if InlineSource.is_inline(token):
            return InlineSource(token)
        
        parts = token.split(',') # the separator for optional params
        path = parts[0]
        parms = parts[1] if len(parts) > 1 else ""

        if path.startswith('s3:'):
            return S3Source.create(path, cls.source_class_getter, parms)

        elif os.path.isdir(path):
            return DirSource.create(path, cls.source_class_getter, parms)


        # individual file
        source_class = cls.source_class_getter(path, parms)
        if source_class:
            lazy_file = LazyFileLocal(path)
            return source_class(lazy_file)
     
        return None
