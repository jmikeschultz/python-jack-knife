# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
import subprocess
import shutil
from pjk.base import Sink, Source, ParsedToken, Usage

class YamlSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='yaml',
            desc='Write all records to a YAML file as multi-doc stream'
        )
        usage.def_arg(name='path', usage='Path to output YAML file')
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.path = usage.get_arg('path')

    def process(self) -> None:
        with open(self.path, 'w') as f:
            yaml.dump_all(self.input, f, sort_keys=False)


class StdoutYamlSink(Sink):
    # No usage() — not token-based; intended for internal use
    def __init__(self, input_source: Source, use_pager: bool = True):
        super().__init__(input_source)
        self.use_pager = use_pager
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
