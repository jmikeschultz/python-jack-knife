# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys, shutil, subprocess, contextlib, signal
import os
import yaml

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

class Lookups:
    def __init__(self):
        self.lookups_yaml = os.path.expanduser('~/.pjk/lookups.yaml')
        self._data = {}
        self._load()

    def _load(self):
        """Load lookups from YAML file if it exists."""
        if os.path.exists(self.lookups_yaml):
            with open(self.lookups_yaml, 'r') as f:
                self._data = yaml.safe_load(f) or {}
        else:
            self._data = {}

    def save(self):
        """Save current lookups back to YAML file."""
        os.makedirs(os.path.dirname(self.lookups_yaml), exist_ok=True)
        with open(self.lookups_yaml, 'w') as f:
            yaml.safe_dump(self._data, f)

    def get(self, key, default=None):
        """Retrieve a lookup value by key."""
        return self._data.get(key, default)

    def set(self, key, value):
        """Set a lookup value and persist it."""
        self._data[key] = value
        self.save()

    def delete(self, key):
        """Remove a key if it exists and save."""
        if key in self._data:
            del self._data[key]
            self.save()

    def all(self):
        """Return the full lookup dictionary."""
        return dict(self._data)

class ComponentFactory:
    def __init__(self, components: dict, comp_type_name: str):
        self.num_orig = 0
        self.components = components # name -> component_class
        self.comp_type_name = comp_type_name
        self.num_orig_comps = len(components)

    def register(self, name, comp_class):
        self.components[name] = comp_class

    def get_comp_type_name(self):
        return self.comp_type_name

    def print_descriptions(self):
        header = highlight(f'{self.comp_type_name}s')
        print(header)

        i = 0
        plugin = ''
        for name, comp_class in self.components.items():
            usage = comp_class.usage()
            lines = usage.desc.split('\n')
            if i >= self.num_orig_comps:
                plugin = '(~/.pjk/plugin)'
            line = f'  {name:<12} {lines[0]} {plugin}'
            line = highlight(line, 'bold', plugin) if plugin else line
            print(line)
            i += 1

    def get_usage(self, name: str):
        comp_class = self.components.get(name)
        if not comp_class:
            return None
        return comp_class.usage()

    def create(self, token: str):
        pass
