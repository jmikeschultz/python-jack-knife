#!/usr/bin/env python
import sys
from djk.parser import DjkExpressionParser

import sys
import os
from datetime import datetime, timezone
import concurrent.futures

def write_history(tokens):
    log_path = ".pjk-history.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    command = " ".join(tokens)
    with open(log_path, "a") as f:
        f.write(f"{timestamp}\tpjk {command}\n")

def execute_threaded(sinks):
    max_threads = os.cpu_count()
    for _ in range(max_threads - 1):
        clone = sinks[0].deep_copy()
        if not clone:
            break
        sinks.append(clone)

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
    if len(sys.argv) < 2:
        print("Usage: pjk <token1> <token2> ...")
        return

    tokens = sys.argv[1:]
    parser = DjkExpressionParser(tokens)

    try:
        # Build initial sink
        sink = parser.parse()

        clone = sink.deep_copy()
        if clone:
            sinks = [sink]
            sinks.append(clone)
            execute_threaded(sinks)

        sink.drain()

    except SyntaxError as e:
         print(e)
      
    write_history(sys.argv[1:])

if __name__ == "__main__":
    main()