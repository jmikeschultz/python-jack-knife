# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import sys
from pjk.sinks.factory import SinkFactory
from pjk.pipes.factory import PipeFactory
from pjk.sources.factory import SourceFactory
from pjk.sinks.format_sink import FormatSink
from pjk.sources.format_source import FormatSource
import importlib.util
import importlib
import importlib.metadata
from pjk.base import Pipe, Source, Sink, Integration
from pjk.common import ComponentFactory, highlight
from typing import List

class DisplayHolder:
    def __init__(self, factories: List[ComponentFactory]):
        for factory in factories:
            pass            
                 

class ComponentRegistry:
    def __init__(self):
        self.source_factory = SourceFactory()
        self.pipe_factory = PipeFactory()
        self.sink_factory = SinkFactory()

        self.load_user_components()
        load_package_extras()

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

        print_core_formats([self.source_factory, self.sink_factory])
        print()
        print_factory_core(self.source_factory, header='sources')
        print()
        print_factory_core(self.pipe_factory, header='pipes')
        print()
        print_factory_core(self.sink_factory, header='sinks')

        self.print_origin_components('integration', 'integrations')
        self.print_origin_components('user', 'user components (~/.pjk/plugins)')
        
    def load_user_components(self, path=os.path.expanduser("~/.pjk/plugins")):
        if not os.path.isdir(path):
            return

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
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
            except Exception as e:
                print(f"[pjk] Failed to load {fname} from ~/.pjk/plugins: {e}")
                continue

            for obj in vars(module).values():
                if not isinstance(obj, type):
                    continue
                if hasattr(obj, "usage"):
                    usage = obj.usage()
                    name = usage.name

                    if is_sink(obj, module):
                        self.sink_factory.register(name, obj, 'user')
                    elif is_pipe(obj, module):
                        self.pipe_factory.register(name, obj, 'user')
                    elif is_source(obj, module):
                        self.source_factory.register(name, obj, 'user')

    def print_origin_components(self, origin: str, header:str):
        component_tuples = []
        for factory in [self.source_factory, self.pipe_factory, self.sink_factory]:
            component_tuples.extend(factory.get_component_name_class_tuples(origin))

        if not component_tuples:
            return
        print()        
        print(highlight(header))

        for name, comp_class in component_tuples:
            usage = comp_class.usage()
            comp_class_type_str = get_component_type(comp_class)
            lines = usage.desc.split('\n')
            temp = highlight(comp_class_type_str)
            line = f'  {name:<17} {temp:<15} {lines[0]}'
            print(line)

def print_core_formats(factories: List[ComponentFactory]):
    print(highlight('formats'))
    formats = set()
    for factory in factories:
        tuples = factory.get_component_name_class_tuples('core')
        for name, comp_class in tuples:
            if issubclass(comp_class, FormatSink|FormatSource):
                formats.add(name)
    
    space = ' '
    lst = ', '.join(list(formats))
    print(f'{space:<15}{lst}. (sources/sinks in local files, dirs and s3)')

def print_factory_core(factory: ComponentFactory, header: str, include_formats: bool=False, include_integrations=False):
        components:list = factory.get_component_name_class_tuples('core')
        header = highlight(header)
        print(header)

        i = 0
        # user and outside package components are also here, but printed from registry class
        for name, comp_class in components:
            if issubclass(comp_class, FormatSink|FormatSource) and not include_formats:
                continue

            usage = comp_class.usage()
            lines = usage.desc.split('\n')

            line = f'  {name:<12} {lines[0]}'
            print(line)
            i += 1
    
def get_component_type(component_class) -> str:
    if issubclass(component_class, Sink):
        return 'sink'
    elif issubclass(component_class, Pipe):
        return 'pipe'
    elif issubclass(component_class, Source):
        return 'source'
    return 'unknown'

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
