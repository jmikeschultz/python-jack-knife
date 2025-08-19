# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.sources.csv_source import CSVSource
from djk.sources.lazy_file import LazyFile
from djk.sources.format_usage import FormatUsage

class TSVSource(CSVSource):
    is_format = True
    @classmethod
    def usage(cls):
        return FormatUsage('csv', 'csv source for local files and directories and also s3', component_class=cls)
    
    def __init__(self, lazy_file: LazyFile):
        super().__init__(lazy_file, delimiter="\t")
