# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.components import Sink
from pjk.usage import ParsedToken, Usage, TokenError

from pjk.sinks.graph_cumulative import graph_cumulative
from pjk.sinks.graph_hist import graph_hist
from pjk.sinks.graph_scatter import graph_scatter
from pjk.sinks.graph_bar_line import graph_bar_line

class GraphSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='graph',
            desc='Display various kinds of graphs.',
            component_class=cls
        )
        usage.def_arg(name='kind', usage='hist|scatter|bar|line|cumulative')
        usage.def_param(name='x', usage='x-axis field', default='x')
        usage.def_param(name='y', usage='comma separated list of y-axis fields', default='y')
        usage.def_param(name='pause', usage='Seconds to show graph', is_num=True, default='-1')
        usage.def_param(name='title', usage='A title for the graph', is_num=False)
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.records = []
        self.kind = usage.get_arg('kind')
        self.x_field = usage.get_param('x')
        self.y_field = usage.get_param('y')
        self.pause = usage.get_param('pause')
        self.title = usage.get_param('title')

    def process(self):
        import matplotlib.pyplot as plt # lazy import

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
        
        if not self.pause:
            plt.show()
        else:
            plt.show(block=False)   
            plt.pause(int(self.pause))           
            plt.close()            
