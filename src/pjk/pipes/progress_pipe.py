from typing import Iterator
from pjk.base import Pipe
from pjk.progress import papi

# monitors flow of records wherever inserted

class ProgressPipe(Pipe):
    def __init__(self, component_instance = None, simple: bool = False):
        super().__init__(None, None)
        self.component_instance = component_instance
        self.simple = simple

        label = self.get_component_label(component_instance)
        self.counter = papi.get_counter(label, var_label='recs')
        #papi.add_rate(sink_name, self.counter, var_label='krecs/sec')
        if not simple:
            papi.get_counter(label, var_label='threads').increment()
            papi.add_elapsed_time(label, var_label='elapsed')

    def get_component_label(self, component_instance):
        if hasattr(type(component_instance), 'extension'):
            return type(component_instance).extension
        elif hasattr(component_instance, 'usage'):
            return type(component_instance).usage().name
        return type(component_instance).__name__

    def __iter__(self) -> Iterator:
        # only counting here
        for record in self.left:
            self.counter.increment()
            yield record

    def deep_copy(self):
        source_clone = self.left.deep_copy()
        if not source_clone:
            return None

        pipe = ProgressPipe(self.component_instance, self.simple)
        pipe.add_source(source_clone)
        return pipe

