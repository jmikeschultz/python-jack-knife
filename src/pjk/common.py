# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import contextlib, io, os, subprocess, sys
import os
import re
from abc import ABC
from enum import Enum
from pjk.sources.format_source import FormatSource
from pjk.sinks.format_sink import FormatSink
from typing import List, Type

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

# pjk/common.py
import contextlib, io, os, subprocess, sys

@contextlib.contextmanager
def pager_stdout(use_pager: bool = True):
    """
    Stream stdout into `less` via a pipe.
    - If stdout is not a TTY or use_pager is False → write directly to sys.stdout.
    - Otherwise spawn `less` and replace sys.stdout with less.stdin.
    """
    # If not a TTY, paging makes no sense
    if not use_pager or not sys.stdout.isatty():
        yield
        return

    env = os.environ.copy()
    # -R: pass ANSI; -S: chop long lines; you can add -F/-X to taste
    env.setdefault("LESS", "-RFX")
    # Ensure UTF-8
    env.setdefault("LESSCHARSET", "utf-8")

    stdout_orig = sys.stdout
    stderr_orig = sys.stderr

    # Start less with a *pipe* for stdin and inherit the real terminal for out/err
    pager = subprocess.Popen(
        ["less"],
        stdin=subprocess.PIPE,
        stdout=stdout_orig,   # keep interactivity
        stderr=stderr_orig,
        env=env,
        close_fds=True,
        bufsize=0,            # unbuffered pipe
    )

    # Wrap less.stdin as a text writer and swap sys.stdout
    assert pager.stdin is not None
    pager_bin = pager.stdin
    pager_txt = io.TextIOWrapper(pager_bin, encoding="utf-8", write_through=True)

    sys.stdout = pager_txt
    try:
        yield
    except BrokenPipeError:
        pass
    finally:
        try:
            sys.stdout.flush()
        except Exception:
            pass
        # Restore first, then close pager stdin to send EOF
        sys.stdout = stdout_orig
        try:
            pager_txt.flush()
        except Exception:
            pass
        try:
            pager_bin.close()   # EOF → lets less exit
        except Exception:
            pass
        try:
            pager.wait()
        except Exception:
            pass

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

# mixin
class Integration(ABC):
    pass

class ComponentOrigin(Enum):
    CORE = 0 # core components defined in python-jack-knife
    EXTERNAL = 1 # component loaded via load_package_extras (displayed in either 'integrations' or 'applications')
    USER = 2 # components loaded via load_user_components (always displayed in user_components)

class ComponentWrapper:
    def __init__(self, name: str, comp_class, origin: ComponentOrigin):
        self.name = name
        self.comp_class = comp_class
        self.origin = origin
        self.is_integration = issubclass(comp_class, Integration)

class ComponentFactory:
    def __init__(self, core_components: dict):
        self.wrappers = {}
        for k, v in core_components.items():
            self.register(k, v, origin=ComponentOrigin.CORE)

    def register(self, name, comp_class, origin: ComponentOrigin):
        self.wrappers[name] = ComponentWrapper(name, comp_class=comp_class, origin=origin)

    # is_integration True|False|None=don't care
    def get_components(self, origin_list: List[ComponentOrigin], is_integration: bool) -> dict:
        all = {}
        for wrapper in self.wrappers.values():
            if is_integration is not None:
                if wrapper.is_integration != is_integration:
                    continue

            for o in origin_list:
                if wrapper.origin == o:
                    all[wrapper.name] = wrapper.comp_class

        return all

    def get_component_class(self, name: str):
        wrapper = self.wrappers.get(name, None)
        if not wrapper:
            return None
        return wrapper.comp_class

    def create(self, token: str):
        pass

def is_valid_field_name(name: str):
    return re.fullmatch(r'^[A-Za-z_][A-Za-z0-9_]*$', name)