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
from datetime import datetime
import traceback
import concurrent.futures
from pjk.registry import ComponentRegistry
from pjk.sinks.stdout import StdoutSink
from pjk.man_page import do_man, do_examples
from pjk.sinks.expect import ExpectSink
from pjk.progress import ProgressDisplay
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
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)  # no 'with'
    futures = {executor.submit(s.drain): s for s in sinks}
    try:
        for future in concurrent.futures.as_completed(futures):
            sink_obj = futures[future]
            future.result()  # re-raises worker exception with traceback
    except KeyboardInterrupt:
        # stop UI first, then cancel and non-blocking shutdown
        if stop_progress:
            try: stop_progress()
            except Exception: pass
        for f in futures:
            f.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        os._exit(130)

    except Exception as e:
        if stop_progress:
            try: stop_progress()
            except Exception: pass
        sys.stderr.write(f"Sink {futures[future]} raised an exception:\n")
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
        for f in futures:
            f.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

def execute(command: str):
    tokens = shlex.split(command, comments=True, posix=True)
    execute_tokens(tokens)

def execute_tokens(tokens: List[str]):
    init_logging()
    # (remove the sys.exit SIGINT handler here)

    if '--version' in tokens:
        print(f"pjk version {__version__}")
        sys.exit(0)

    registry = ComponentRegistry()

    if len(tokens) < 1:
        registry.print_usage()
        return

    if len(tokens) == 2 and tokens[0] == 'man':
        do_man(tokens[1], registry)
        return

    if len(tokens) == 1 and tokens[0] in ['examples', 'examples+']:
        do_examples(tokens[0], registry)
        return

    parser = ExpressionParser(registry)

    display = None
    try:
        sink = parser.parse(tokens)
        if not isinstance(sink, (StdoutSink | ExpectSink)):
            display = ProgressDisplay(interval=3.0)
            display.start()

        sinks = [sink]
        max_threads = os.cpu_count()
        while len(sinks) < max_threads:
            clone = sink.deep_copy()
            if not clone:
                break
            sinks.append(clone)

        if len(sinks) > 1:
            # pass a stopper so we halt the UI before tracebacks / shutdown
            execute_threaded(sinks, stop_progress=(display.stop if display else None))
        else:
            sink.drain()

        write_history(sys.argv[1:])

    except UsageError as e:
        print(e, file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        pass
    finally:
        if display:
            # short join so Ctrl-C is immediate
            try: display.stop(timeout=0.1)
            except Exception: pass

def main():
    tokens = sys.argv[1:]
    execute_tokens(tokens)

if __name__ == "__main__":
    main()
