from djk.base import Sink, Source, ParsedToken, Usage, TokenError
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt

from djk.sinks.graph_cumulative import graph_cumulative
from djk.sinks.graph_hist import graph_hist
from djk.sinks.graph_scatter import graph_scatter
from djk.sinks.graph_bar_line import graph_bar_line

class GraphSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='graph',
            desc='display graphs of various kinds'
        )
        usage.def_arg(name='kind', usage='hist|scatter|bar|line|cumulative')
        usage.def_arg(name='x-field', usage='name of x-axis field')
        usage.def_arg(name='y-field', usage='name of y-axis field')
        usage.def_param(name='pause', usage='seconds to show graph, otherwise indefinite.', is_num=True)
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.records = []
        self.kind = usage.get_arg('kind')
        self.x_field = usage.get_arg('x-field')
        self.y_field = usage.get_arg('y-field')
        self.pause = usage.get_param('pause')

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
            raise TokenError(f"Unsupported graph type: {self.kind}")

    
