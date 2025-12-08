# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.components import Source
from pjk.usage import ParsedToken
from pjk.common import ComponentFactory
from pjk.sources.json_source import JsonSource
from pjk.sources.csv_source import CSVSource
from pjk.sources.sql_source import SQLSource
from pjk.sources.tsv_source import TSVSource
from pjk.sources.npy_source import NpySource
from pjk.sources.inline_source import InlineSource
from pjk.sources.user_source_factory import UserSourceFactory
from pjk.sources.parquet_source import ParquetSource
from pjk.sources.format_source import FormatSource
from pjk.sources.s3_select_source import S3SelectSource

COMPONENTS = {
        'inline': InlineSource,
        's3s': S3SelectSource,
        'json': JsonSource,
        'jsonl': JsonSource,
        'csv': CSVSource,
        'tsv': TSVSource,
        'sql': SQLSource,
        'npy': NpySource,
        'parquet': ParquetSource,
    }

class SourceFactory(ComponentFactory):
    def __init__(self):
        super().__init__(COMPONENTS)
    
    def get_comp_type_name(self):
        return 'source'

    def create(self, token: str) -> Source:
        token = token.strip()

        # s3s is a pseudo source only in the above list to provide easy man page
        # it's instantiated by the parser when <file>.s3s, so disallow standard search for it.
        if token == 's3s':
            return None

        if InlineSource.is_inline(token):
            return InlineSource(token)
        
        ptok = ParsedToken(token)
        
        if ptok.pre_colon.endswith('.py'):
            source = UserSourceFactory.create(ptok)
            if source:
                return source
            
        # s3 select file
        if ptok.pre_colon.endswith('.s3s'):
            source = S3SelectSource(ptok, None)
            if source:
                return source

        source_cls = self.get_component_class(ptok.pre_colon)
        if source_cls and not issubclass(source_cls, FormatSource):
            usage = source_cls.usage()
            usage.bind(ptok)
        
            source = source_cls(ptok, usage)
            return source
        
        return FormatSource.create(ptok, COMPONENTS)
    