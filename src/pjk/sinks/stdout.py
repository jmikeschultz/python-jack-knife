# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
from yaml.representer import SafeRepresenter
from pjk.components import Sink, Source
from pjk.usage import ParsedToken, Usage
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
        self.use_pager = True if usage.get_param('less') == None else usage.get_param('less') == 'true'

    def process(self) -> None:
        try:
            with pager_stdout(self.use_pager):
                for record in self.input:
                    try:
                        # everything else -> normal YAML
                        yaml.dump(
                            record,
                            sys.stdout,
                            sort_keys=False,
                            explicit_start=True,
                            allow_unicode=True,
                            width=10**9,
                        )
                    except BrokenPipeError:
                        break
        except BrokenPipeError:
            pass


