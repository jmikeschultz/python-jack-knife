# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
from pjk.base import Sink, Source, ParsedToken, Usage
from pjk.common import pager_stdout

class StdoutSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='-',
            desc='display records in yaml format to stdout through less',
            component_class=cls
        )
        usage.def_param('less', usage='use less to display', valid_values=['true', 'false'], default='true')
        usage.def_example(["{hello:'world!'}"], "{hello:'world!'}")
        return usage
    
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        # NOTE: self.use_pager is hardcoded for now; override via constructor if needed
        self.use_pager = True if usage.get_param('less') == None else usage.get_param('less') == 'true'

    def process(self) -> None:
        # Route all stdout into `less` while dumping
        try:
            with pager_stdout(self.use_pager):
                for record in self.input:
                    try:
                        yaml.dump(
                            record,
                            sys.stdout,          # now points to less (if enabled)
                            sort_keys=False,
                            explicit_start=True,
                            width=10**9          # effectively no wrap without using a float
                        )
                    except BrokenPipeError:
                        break  # user quit pager
        except BrokenPipeError:
            # Swallow if pager closed early
            pass
