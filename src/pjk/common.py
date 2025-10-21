# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys, shutil, subprocess, contextlib, signal
import os
import re
import yaml
from pjk.base import TokenError, Integration, Source, Pipe

class SafeNamespace:
    def __init__(self, obj):
        for k, v in obj.items():
            if isinstance(v, dict):
                v = SafeNamespace(v)
            elif isinstance(v, list):
                v = [SafeNamespace(x) if isinstance(x, dict) else x for x in v]
            setattr(self, k, v)

    def __getattr__(self, key):
        return None  # gracefully handle missing keys

class ReducingNamespace:
    def __init__(self, record):
        self._record = record

    def __getattr__(self, name):
        value = self._record[name]
        if isinstance(value, (list, tuple, set)):
            return value
        return [value]  # promote scalars to singleton lists

@contextlib.contextmanager
def pager_stdout(use_pager=True):
    if use_pager and shutil.which("less"):
        # Avoid BrokenPipeError noise if user quits less early
        try:
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        except Exception:
            pass  # not available on Windows

        pager = subprocess.Popen(["less", "-FRSX"], stdin=subprocess.PIPE, text=True)
        old_stdout = sys.stdout
        try:
            sys.stdout = pager.stdin
            yield
        finally:
            try:
                sys.stdout.flush()
            except Exception:
                pass
            sys.stdout = old_stdout
            if pager.stdin:
                pager.stdin.close()
            pager.wait()
    else:
        yield

COLOR_CODES = {
        'bold': '\033[1m',
        'underline': '\033[4m',
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'blue': '\033[34m',
        'magenta': '\033[35m',
        'cyan': '\033[36m',
        'gray': '\033[90m',
    }

RESET = '\033[0m'

def highlight(text: str, color: str = 'bold', value: str = None) -> str:
    value = text if not value else value
    style = COLOR_CODES.get(color.lower(), COLOR_CODES['bold'])
    return text.replace(value, f"{style}{value}{RESET}")

class Config:
    def __init__(self, instance_type: str, component_class: Source|Pipe, instance: str):
        # instance = name of the instance, e.g. 'myindexcollection', instance_type = 'index'
        # instance_type only used by automatic config template maker MUST BE STRING LITERAL!
        self.configs_yaml = os.path.expanduser('~/.pjk/component_configs.yaml')
        self.class_name = type(component_class).__name__
        self.instance = instance
        self._data = {}
        self._load()
        
    def _load(self):
        if os.path.exists(self.configs_yaml):
            with open(self.configs_yaml, 'r') as f:
                self._data = yaml.safe_load(f) or {}
        else:
            self._data = {}

    def lookup(self, param: str, default=None):
        instance_key = f'{self.class_name}-{self.instance}'
        entry = self._data.get(instance_key, None)
        if not entry:
            raise TokenError(
                f"~/.pjk/component_configs.yaml does not contain entry for '{instance_key}' with required params."
            )
        
        return entry.get(param, default)

class ComponentFactory:
    def __init__(self, core_components: dict):
        self.num_orig = 0
        self._components = {}
        for k, v in core_components.items():
            if issubclass(v, Integration):
                self.register(k, v, 'integration')
            else:
                self.register(k, v, 'core')

    def register(self, name, comp_class, origin: str):
        self._components[name] = (comp_class, origin)

    def get_comp_type_name(self):
        pass

    def get_component_name_class_tuples(self, origin: str = None) -> list:
        ret = []
        for k, (v, org) in self._components.items():
            if not origin or origin == org:
                ret.append((k, v))
        return ret

    def get_component_class(self, name: str):
        tuple = self._components.get(name)
        if not tuple:
            return None
        component_class, origin = tuple
        return component_class

    def get_usage(self, name: str):
        comp_class = self.get_component_class(name)
        if not comp_class:
            return None
        return comp_class.usage()

    def create(self, token: str):
        pass

def is_valid_field_name(name: str):
    return re.fullmatch(r'^[A-Za-z_][A-Za-z0-9_]*$', name)