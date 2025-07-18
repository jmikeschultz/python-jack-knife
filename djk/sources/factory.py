import os
import queue
from djk.base import Source, ParsedToken, ComponentFactory
from djk.sources.json_source import JsonSource
from djk.sources.csv_source import CSVSource
from djk.sources.tsv_source import TSVSource
from djk.sources.s3_source import S3Source
from djk.sources.source_list import SourceListSource
from djk.sources.inline_source import InlineSource
from djk.sources.dir_source import DirSource
from djk.sources.user_source_factory import UserSourceFactory
from djk.sources.lazy_file import LazyFile
from djk.sources.lazy_file_local import LazyFileLocal
#from djk.sources.postgres import PostgresSource

class SourceFactory(ComponentFactory):
    HEADER = 'SOURCES'
    COMPONENTS = {
        'inline': InlineSource,
        'json': JsonSource,
        'cvs': CSVSource,
        'tsv': TSVSource,
        's3': S3Source
    }

    @classmethod
    def get_format_class_gz(cls, ptok: ParsedToken):
        params = ptok.get_params()
        override = params.get('format', None) # e.g. json or json.gz

        lookup = None

        is_gz = False
        if override:
            if override.endswith('.gz'):
                is_gz = True
                override = override.removesuffix('.gz')
            lookup = override

        else: # e.g. foo.json or foo.json.gz
            path = ptok.main
            if path.endswith('.gz'):
                is_gz = True
                path = path.removesuffix('.gz')

            path, ext = os.path.splitext(path) # e.g path=foo.json
            lookup = ext.removeprefix('.')
            
        format_class = cls.COMPONENTS.get(lookup, None)
        if not format_class:
            return None, None
        
        # make sure
        if not format_class.is_format:
            return None, None # raise ?

        return format_class, is_gz

    @classmethod
    def create(cls, token: str) -> Source:
        token = token.strip()

        if InlineSource.is_inline(token):
            return InlineSource(token)
        
        ptok = ParsedToken(token)
        
        if ptok.main.endswith('.py'):
            source = UserSourceFactory.create(ptok)
            if source:
                return source

        if ptok.main.startswith('s3:'):
            return S3Source.create(ptok)

        if os.path.isdir(ptok.main):
            return DirSource.create(ptok, get_format_class_gz=cls.get_format_class_gz)

        # individual file
        if os.path.isfile(ptok.main):
            source_class, is_gz = cls.get_format_class_gz(ptok)
            if source_class:
                lazy_file = LazyFileLocal(ptok.main, is_gz)
                return source_class(lazy_file)
     
        return None
