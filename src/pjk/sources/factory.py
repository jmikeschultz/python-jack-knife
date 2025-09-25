# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import queue
from pjk.base import Source, ParsedToken
from pjk.common import ComponentFactory
from pjk.sources.json_source import JsonSource
from pjk.sources.csv_source import CSVSource
from pjk.sources.sql_source import SQLSource
from pjk.sources.tsv_source import TSVSource
from pjk.sources.s3_source import S3Source
from pjk.sources.inline_source import InlineSource
from pjk.sources.dir_source import DirSource
from pjk.sources.user_source_factory import UserSourceFactory
from pjk.sources.lazy_file import LazyFile
from pjk.sources.lazy_file_local import LazyFileLocal
from pjk.sources.parquet_source import ParquetSource
from pjk.sources.format_source import FormatSource

COMPONENTS = {
        'inline': InlineSource,
        'json': JsonSource,
        'csv': CSVSource,
        'tsv': TSVSource,
        'sql': SQLSource,
        'parquet': ParquetSource,
    }

class SourceFactory(ComponentFactory):
    def __init__(self):
        super().__init__(COMPONENTS, 'source')
    

    def get_format_class_gz(self, ptok: ParsedToken):
        params = ptok.get_params()
        override = params.get('format', None) # e.g. json or json.gz

        lookup = None

        is_gz = ptok.all_but_params.endswith('gz')
        if override:
            if override.endswith('.gz'):
                is_gz = True
                override = override.removesuffix('.gz')
            lookup = override

        else: # e.g. foo.json or foo.json.gz
            path = ptok.all_but_params
            if path.endswith('.gz'):
                is_gz = True
                path = path.removesuffix('.gz')

            path, ext = os.path.splitext(path) # e.g path=foo.json
            lookup = ext.removeprefix('.')
            
        format_class = self.components.get(lookup, None)
        if not format_class:
            return None, None
        
        # make sure
        if not issubclass(format_class, FormatSource):
            return None, None # raise ?

        return format_class, is_gz

    def create(self, token: str) -> Source:
        token = token.strip()

        if InlineSource.is_inline(token):
            return InlineSource(token)
        
        ptok = ParsedToken(token)
        
        if ptok.pre_colon.endswith('.py'):
            source = UserSourceFactory.create(ptok)
            if source:
                return source

        source_cls = self.components.get(ptok.pre_colon)
        if source_cls and not issubclass(source_cls, FormatSource):
            usage = source_cls.usage()
            usage.bind(ptok)
        
            source = source_cls(ptok, usage)
            return source
        
        return FormatSource.create(ptok, COMPONENTS)
    