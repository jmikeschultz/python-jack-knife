import os
from djk.base import Source, Pipe, PipeSyntaxError
from djk.sources.json_source import JsonSource
from djk.sources.csv_source import CSVSource
from djk.sources.s3_source_factory import S3SourceFactory
from djk.sources.source_list import SourceListSource
from djk.sources.inline_source import InlineSource

from djk.sources.lazy_file import LazyFile
from djk.sources.lazy_file_local import LazyFileLocal

class SourceFactory:

    @classmethod
    def _resolve(cls, name, parms) -> Source:
        if name.endswith(".json") or name.endswith(".json.gz") or 'format=json' in parms:
            return JsonSource
        if name.endswith(".csv") or name.endswith(".csv.gz") or 'format=csv' in parms:
            return CSVSource
            
        return None

    @classmethod
    def get_s3_source(cls, path, parms):
        s3factory = S3SourceFactory(path, parms, cls._resolve)
        return s3factory.create()

    @classmethod
    def get_dir_source(cls, path, parms):
        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        sources = []
        for file in files:
            source_class = cls._resolve(file, parms)
            if source_class:
                lazy_file = LazyFileLocal(file)
                sources.append(source_class(lazy_file))
            else:
                print(f'fix me in source factory:{file}')
                break
            
        if not sources:
            return None
            
        return SourceListSource(iter(sources))


    @classmethod
    def create(cls, token) -> Source:
        token = token.strip()

        if InlineSource.is_inline(token):
            return InlineSource(token)
        
        parts = token.split(',') # the separator for optional params
        path = parts[0]
        parms = parts[1] if len(parts) > 1 else ""

        if path.startswith('s3:'):
            return cls.get_s3_source(path, parms)

        elif os.path.isdir(path):
            return cls.get_dir_source(path, parms)

        # individual file
        source_class = cls._resolve(path, parms)
        if source_class:
            lazy_file = LazyFileLocal(path)
            return source_class(lazy_file)
     
        return None
