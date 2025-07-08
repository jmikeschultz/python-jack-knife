from djk.sinks.csv_sink import CSVSink
from djk.base import Source

class TSVSink(CSVSink):
    def __init__(self, input_source: Source, path_no_ext: str):
        super().__init__(input_source, path_no_ext, delimiter="\t", ext='tsv')
