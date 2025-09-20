# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from pjk.sinks.factory import SinkFactory
from pjk.pipes.factory import PipeFactory
from pjk.sources.factory import SourceFactory
import importlib.util
import importlib
import importlib.metadata
from pjk.base import Pipe, Source, Sink

class ComponentRegistry:
    def __init__(self):
        self.source_factory = SourceFactory()
        self.pipe_factory = PipeFactory()
        self.sink_factory = SinkFactory()

        sources, pipes, sinks = load_user_components()
        for name, comp in sources.items():
            self.source_factory.register(name, comp)

        for name, comp in pipes.items():
            self.pipe_factory.register(name, comp)

        for name, comp in sinks.items():
            self.sink_factory.register(name, comp)

        load_package_extras()

    def register(self, name, comp):
        if is_pipe(comp):
            if hasattr(comp, "usage"):
                usage = comp.usage()
                name = usage.name
            self.pipe_factory.register(name, comp)
        elif is_sink(comp):
            self.sink_factory.register(name, comp)
        elif is_source(comp):
            self.source_factory(name, comp)

    def create_source(self, token: str):
        return self.source_factory.create(token)
    
    def create_pipe(self, token: str):
        return self.pipe_factory.create(token)
    
    def create_sink(self, token: str):
        return self.sink_factory.create(token)
    
    def get_factories(self):
        return [self.source_factory, self.pipe_factory, self.sink_factory]

    def print_usage(self):
        print('Usage: pjk <source> [<pipe> ...] <sink>')
        print('       pjk man <component> | --all')
        print('       pjk examples')
        print()
        self.source_factory.print_descriptions()
        print()
        self.pipe_factory.print_descriptions()
        print()
        self.sink_factory.print_descriptions()

        return
    
def is_source(obj, module):
    return (
        isinstance(obj, type)
        and issubclass(obj, Source)
        and not issubclass(obj, Pipe)
        and not issubclass(obj, Sink)
        and obj is not Source
        and obj.__module__ == module.__name__  # 🧠 only user-defined classes
        )

def is_pipe(obj, module):
    return (
        isinstance(obj, type)
        and issubclass(obj, Pipe)
        and not issubclass(obj, Sink)
        and obj is not Pipe
        and obj.__module__ == module.__name__
    )

def is_sink(obj, module):
     return (
        isinstance(obj, type)
        and issubclass(obj, Sink)
        and obj is not Sink
        and obj.__module__ == module.__name__
    )

def load_user_components(path=os.path.expanduser("~/.pjk/plugins")):
    sources = {}
    pipes = {}
    sinks = {}

    if not os.path.isdir(path):
        return {}, {}, {}

    for fname in os.listdir(path):
        if not fname.endswith(".py"):
            continue
        fpath = os.path.join(path, fname)
        modname = f"user_component_{fname[:-3]}"
        spec = importlib.util.spec_from_file_location(modname, fpath)
        if not spec or not spec.loader:
            continue
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(f"[djk] Failed to load {fname}: {e}")
            continue

        for obj in vars(module).values():
            if not isinstance(obj, type):
                continue
            if hasattr(obj, "usage"):
                usage = obj.usage()
                name = usage.name

                if is_sink(obj, module):
                    sinks[name] = obj
                elif is_pipe(obj, module):
                    pipes[name] = obj
                elif is_source(obj, module):
                    sources[name] = obj

    return sources, pipes, sinks

def iter_entry_points(group: str):
    eps = importlib.metadata.entry_points()
    if hasattr(eps, "select"):
        # Python 3.10+ (importlib.metadata.EntryPoints)
        return eps.select(group=group)
    # Python 3.9 and older
    return eps.get(group, [])

def load_package_extras():
    """
    Discover and import all installed pjk extras (via entry points).
    """
    for ep in iter_entry_points("pjk.package_extras"):
        try:
            importlib.import_module(ep.value)
            print(f"[pjk] loaded package extra: {ep.name} -> {ep.value}")
        except Exception as e:
            print(f"[pjk] failed to load extra {ep.name}: {e}")
