# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

#!/usr/bin/env python
import sys
import signal
from djk.parser import ExpressionParser
from djk.base import UsageError
from djk.log import init as init_logging
import sys
import os
from datetime import datetime, timezone
import concurrent.futures
from djk.pipes.factory import PipeFactory
from djk.sources.factory import SourceFactory
from djk.sinks.factory import SinkFactory
from djk.man_page import do_man, do_examples
from djk.sinks.expect import ExpectSink
from djk.version import __version__

def write_history(tokens):
    log_path = ".pjk-history.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    command = " ".join(tokens)
    with open(log_path, "a") as f:
        f.write(f"{timestamp}\tpjk {command}\n")

def execute_threaded(sinks):
    # Choose a max thread limit (explicitly)
    max_workers = min(32, len(sinks))  # or set a fixed cap like 8

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(s.drain): s for s in sinks
        }

        for future in concurrent.futures.as_completed(futures):
            sink_obj = futures[future]
            try:
                future.result()  # This will re-raise any exception from s.drain()
            except Exception as e:
                print(f"Sink {sink_obj} raised an exception:")
                print(e)

def main():
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    if '--version' in sys.argv:
        print(f"pjk version {__version__}")
        sys.exit(0)
    
    if len(sys.argv) < 2:
        print('Usage: pjk <source> [<pipe> ...] <sink>')
        print('       pjk man <component> or pjk man --all')
        print('       pjk examples')
        print()
        SourceFactory.print_descriptions()
        print()
        PipeFactory.print_descriptions()
        print()
        SinkFactory.print_descriptions()
        return

    init_logging()
    tokens = sys.argv[1:]

    if len(tokens) == 2 and tokens[0] == 'man':
        do_man(tokens[1])
        return
    if len(tokens) == 1 and tokens[0] == 'examples':
        do_examples()
        return

    parser = ExpressionParser(tokens)

    try:
        # Build initial sink
        sink = parser.parse()

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
            sink.print_info() # rarely used, e.g. expect and devnull

        write_history(sys.argv[1:])

    except UsageError as e:
        print(e, file=sys.stderr)
        sys.exit(2)  # Exit with a non-zero code, but no traceback

if __name__ == "__main__":
    main()
