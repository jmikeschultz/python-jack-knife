# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

#!/usr/bin/env python
import sys
import os
import signal
import shlex
from typing import List
from pjk.parser import ExpressionParser
from pjk.base import UsageError
from pjk.log import init as init_logging
from datetime import datetime, timezone
import traceback
import concurrent.futures
from pjk.registry import ComponentRegistry
from pjk.pipes.factory import PipeFactory
from pjk.sources.factory import SourceFactory
from pjk.sinks.factory import SinkFactory
from pjk.man_page import do_man, do_examples
from pjk.sinks.expect import ExpectSink
from pjk.version import __version__

def write_history(tokens):
    if os.environ.get("PJK_NO_HISTORY") == "1":
        return
    
    log_path = ".pjk-history.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    command = " ".join(tokens)

    try:
        with open(log_path, "a") as f:
            f.write(f"{timestamp}\tpjk {command}\n")
    except (PermissionError, OSError):
        pass

def execute_threaded(sinks, stop_progress=None):
    max_workers = min(32, len(sinks))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(s.drain): s for s in sinks}

        for future in concurrent.futures.as_completed(futures):
            sink_obj = futures[future]
            try:
                future.result()  # re-raises worker exception with traceback
            except Exception as e:
                # stop progress UI first so it doesn't overwrite the traceback
                if stop_progress:
                    try: stop_progress()
                    except Exception: pass

                sys.stderr.write(f"Sink {sink_obj} raised an exception:\n")
                traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)

                # cancel remaining work and bail
                for f in futures:
                    f.cancel()
                # if on py3.9+, you can also: executor.shutdown(cancel_futures=True)
                raise

def execute(command: str):
    tokens = shlex.split(command, comments=True, posix=True)
    execute_tokens(tokens)

def execute_tokens(tokens:List[str]):
    init_logging()
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    if '--version' in tokens:
        print(f"pjk version {__version__}")
        sys.exit(0)
    
    registry = ComponentRegistry()
       
    if len(tokens) < 1:
        registry.print_usage()
        return
    
    # pjk man --all | --all+ | <component>
    if len(tokens) == 2 and tokens[0] == 'man':
        do_man(tokens[1], registry)
        return
    
    # pjk examples | examples+
    if len(tokens) == 1 and tokens[0] in ['examples', 'examples+']:
        do_examples(tokens[0], registry)
        return

    parser = ExpressionParser(registry)

    try:
        # Build initial sink
        sink = parser.parse(tokens)

        sinks = [sink]
        max_threads = os.cpu_count()
        while len(sinks) < max_threads:
            clone = sink.deep_copy()
            if not clone:
                break
            sinks.append(clone)

        if len(sinks) > 1:
            execute_threaded(sinks)
        else:
            sink.drain() # run single in main thread

        write_history(sys.argv[1:])

    except UsageError as e:
        print(e, file=sys.stderr)
        sys.exit(2)  # Exit with a non-zero code, but no traceback

def main():
    tokens = sys.argv[1:]
    execute_tokens(tokens)

if __name__ == "__main__":
    main()
