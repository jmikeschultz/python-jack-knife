# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
import subprocess
import shutil
from djk.base import Sink, Source, ParsedToken, Usage

class StdoutSink(Sink):
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        # NOTE: self.use_pager is hardcoded for now; override via constructor if needed
        self.use_pager = True
        self.suppress_report = True

    def process(self) -> None:
        output_stream = sys.stdout
        pager_proc = None

        if self.use_pager and shutil.which("less"):
            pager_proc = subprocess.Popen(
                ["less", "-FRSX"],
                stdin=subprocess.PIPE,
                text=True
            )
            output_stream = pager_proc.stdin

        try:
            for record in self.input:
                try:
                    yaml.dump(
                        record,
                        output_stream,
                        sort_keys=False,
                        explicit_start=True,
                        width=float("inf")
                    )
                except BrokenPipeError:
                    break  # user quit pager
        except BrokenPipeError:
            pass
        finally:
            if pager_proc:
                try:
                    output_stream.close()
                except BrokenPipeError:
                    pass
                pager_proc.wait()
