from djk.sources.csv_source import CSVSource
from djk.sources.lazy_file import LazyFile

class TSVSource(CSVSource):
    is_format = True
    
    def __init__(self, lazy_file: LazyFile):
        super().__init__(lazy_file, delimiter="\t")
