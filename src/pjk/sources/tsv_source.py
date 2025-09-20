# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.sources.csv_source import CSVSource
from pjk.sources.lazy_file import LazyFile

class TSVSource(CSVSource):
    extension = 'tsv'

    def __init__(self, lazy_file: LazyFile):
        super().__init__(lazy_file, delimiter="\t")
