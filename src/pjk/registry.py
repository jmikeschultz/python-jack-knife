# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import sys
import time
from pjk.sinks.factory import SinkFactory
from pjk.pipes.factory import PipeFactory
from pjk.sources.factory import SourceFactory
from pjk.sinks.format_sink import FormatSink
from pjk.sources.format_source import FormatSource
import importlib.util
import importlib
import importlib.metadata
import sysconfig, pathlib, importlib
from pjk.components import Pipe, Source, Sink
from pjk.common import ComponentFactory, highlight, ComponentOrigin
from typing import List, Type

class ExternalRegistrar:
    def __init__(self, sources: SourceFactory, pipes: PipeFactory, sinks: SinkFactory) -> None:
        self._sources: SourceFactory = sources
        self._pipes: PipeFactory = pipes
        self._sinks: SinkFactory = sinks

    def source(self, name: str, cls: Type[Source]) -> None:
        self._sources.register(name, cls, origin=ComponentOrigin.EXTERNAL)

    def pipe(self, name: str, cls: Type[Pipe]) -> None:
        self._pipes.register(name, cls, origin=ComponentOrigin.EXTERNAL)

    def sink(self, name: str, cls: Type[Sink]) -> None:
        self._sinks.register(name, cls, origin=ComponentOrigin.EXTERNAL)

class ComponentRegistry:
    def __init__(self):
        self.source_factory = SourceFactory()
        self.pipe_factory = PipeFactory()
        self.sink_factory = SinkFactory()
        self.load_user_components()
        self.load_package_extras()

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

        self.print_non_core([ComponentOrigin.CORE,ComponentOrigin.EXTERNAL], is_integration=True, header='integrations')
        self.print_non_core([ComponentOrigin.EXTERNAL], is_integration=False, header='apps')
        self.print_non_core([ComponentOrigin.USER], is_integration=None, header='user components (~/.pjk/plugins)')        

    # is_integration = True|False|None  None=don't care
    def print_non_core(self, origin_list: List[ComponentOrigin], is_integration: bool, header:str):
        all = {}
        for factory in [self.source_factory, self.pipe_factory, self.sink_factory]:
            component_dict = factory.get_components(origin_list=origin_list, is_integration=is_integration)
            all.update(component_dict)

        if not all:
            return
        
        print()        
        print(highlight(header))

        for name, comp_class in all.items():
            usage = comp_class.usage()
            comp_class_type_str = get_component_type(comp_class)
            lines = usage.desc.split('\n')
            temp = highlight(comp_class_type_str)
            line = f'  {name:<17} {temp:<15} {lines[0]}'
            print(line)

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
                        self.sink_factory.register(name, obj, ComponentOrigin.USER)
                    elif is_pipe(obj, module):
                        self.pipe_factory.register(name, obj, ComponentOrigin.USER)
                    elif is_source(obj, module):
                        self.source_factory.register(name, obj, ComponentOrigin.USER)

    def iter_entry_points(self, group: str):
        """
        Return entry points in the given group across Python 3.9–3.12.
        """
        eps = importlib.metadata.entry_points()
        return eps.select(group=group) if hasattr(eps, "select") else eps.get(group, [])

    def load_package_extras(self, group: str = "pjk.package_extras") -> None:
        """
        Load pip-installed extras and register their components into THIS registry.

        Preferred contract:
        entry point -> callable register(registrar)

        Fallback (legacy):
        entry point -> module path; we import the MODULE PART ONLY for side-effects.
        """
        registrar = ExternalRegistrar(self.source_factory, self.pipe_factory, self.sink_factory)

        for ep in self.iter_entry_points(group):
            try:
                # Try the modern, explicit path first.
                loader = getattr(ep, "load", None)
                if callable(loader):
                    target = ep.load()  # resolves "module:object" to the actual object
                    if callable(target):
                        target(registrar)  # plugin registers into your live factories
                        #print(f"[pjk] loaded extra (callable): {ep.name} -> {ep.value}")
                        continue
                    # Not callable -> fall through to legacy import
                # Legacy path: import ONLY the module portion before ':' for side-effects
                mod = ep.value.split(":", 1)[0]
                importlib.import_module(mod)
                #print(f"[pjk] loaded extra (import): {ep.name} -> {ep.value}")
            except Exception as e:
                print(f"[pjk] failed to load extra {ep.name}: {e}")

def print_core_formats(factories: List[ComponentFactory]):
    print(highlight('formats'))
    formats = set()
    for factory in factories:
        component_dict = factory.get_components([ComponentOrigin.CORE], is_integration=False)
        for name, comp_class in component_dict.items():
            if issubclass(comp_class, FormatSink|FormatSource):
                formats.add(name)
    
    space = ' '
    lst = ', '.join(list(formats))
    print(f'{space:<15}{lst}. (sources/sinks in local files, dirs and s3)')

def print_factory_core(factory: ComponentFactory, header: str):
    component_dict = factory.get_components([ComponentOrigin.CORE], is_integration=False)
    header = highlight(header)
    print(header)

    # user and outside package components are also here, but printed from registry class
    for name, comp_class in component_dict.items():
        if issubclass(comp_class, FormatSink|FormatSource):
            continue

        usage = comp_class.usage()
        lines = usage.desc.split('\n')

        line = f'  {name:<12} {lines[0]}'
        print(line)
    
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

