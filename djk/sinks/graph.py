# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.base import Sink, Source, ParsedToken, Usage, TokenError
import matplotlib.pyplot as plt
import numpy as np

from djk.sinks.graph_cumulative import graph_cumulative
from djk.sinks.graph_hist import graph_hist
from djk.sinks.graph_scatter import graph_scatter
from djk.sinks.graph_bar_line import graph_bar_line

class GraphSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='graph',
            desc='Display various kinds of graphs from streamed records'
        )
        usage.def_arg(name='kind', usage='hist|scatter|bar|line|cumulative')
        usage.def_param(name='x', usage='Name of x-axis field')
        usage.def_param(name='y', usage='Name of y-axis field')
        usage.def_param(name='pause', usage='Seconds to show graph (default: wait for close)', is_num=True)
        return usage

    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.records = []
        self.kind = usage.get_arg('kind')
        self.x_field = usage.get_param('x')
        self.y_field = usage.get_param('y')
        self.pause = usage.get_param('pause')

    def process(self):
        for record in self.input:
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
