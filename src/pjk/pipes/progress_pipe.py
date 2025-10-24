from typing import Iterator
from pjk.components import Source, Pipe, Sink
from pjk.progress import papi
from pjk.progress import ProgressIgnore

# monitors flow of records wherever inserted

class ProgressPipe(Pipe, ProgressIgnore): # ignores only the component register itself
    def __init__(self, component: Source | Sink, simple: bool = False):
        super().__init__(None, None)
        self.component = component
        self.simple = simple

        counter_label = 'recs_in' if isinstance(component, Sink) else 'recs_out'

        self.counter = papi.get_counter(component, var_label=counter_label)
        #papi.add_rate(sink_name, self.counter, var_label='krecs/sec')
        if not simple:
            papi.get_counter(component, var_label='threads').increment()
            papi.add_elapsed_time(component, var_label='elapsed')

    def __iter__(self) -> Iterator:
        # only counting here
        for record in self.left:
            self.counter.increment()
            yield record

    def deep_copy(self):
        source_clone = self.left.deep_copy()
        if not source_clone:
            return None

        pipe = ProgressPipe(self.component, self.simple)
        pipe.add_source(source_clone)
        return pipe

