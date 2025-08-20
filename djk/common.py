# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.base import Source, Pipe, UsageError
import sys, shutil, subprocess, contextlib, signal

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
