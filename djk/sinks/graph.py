from djk.base import Sink, Source, ParsedToken, Usage
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt

from djk.sinks.graph_cumulative import graph_cumulative
from djk.sinks.graph_hist import graph_hist
from djk.sinks.graph_scatter import graph_scatter
from djk.sinks.graph_bar_line import graph_bar_line

class GraphSink(Sink):
    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.records = []
        self.kind = ptok.get_arg(0)
        self.args_dict = dict(item.split('=') for item in ptok.get_arg(1).split(','))

        self.x_field = self.args_dict.pop('x', None)
        self.y_field = self.args_dict.pop('y', None)

    def process(self):
        while True:
            record = self.input.next()
            if record is None:
                break
            self.records.append(record)

        if self.kind == "scatter":
            graph_scatter(self)

        elif self.kind == "hist":
            graph_hist(self)

        elif self.kind == "cumulative":
            graph_cumulative(self)

        elif self.kind == "bar":
            graph_bar_line(self, 'bar')

        elif self.kind == "line":
            graph_bar_line(self, 'line')

        else:
            raise UsageError(f"Unsupported graph type: {self.kind}")

    
