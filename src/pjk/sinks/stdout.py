# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
from yaml.representer import SafeRepresenter
from pjk.components import Sink, Source
from pjk.usage import ParsedToken, Usage
from pjk.common import pager_stdout

# --- NEW: minimal custom dumper that block-quotes only "tricky" strings ---
class _PjkDumper(yaml.SafeDumper):
    pass

def _needs_block(s: str) -> bool:
    # Strings that are annoying to copy if quoted/escaped:
    # - contain YAML structural chars or JSON-ish payloads
    # - contain colon-space (key-like), braces, brackets, quotes, or backslashes
    # - multi-line strings
    return (
        "\n" in s
        or ": " in s
        or any(c in s for c in "{}[]'\"\\")
    )

def _represent_str(dumper, data: str):
    if _needs_block(data):
        # Literal block scalar preserves content exactly, no escaping
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    # Default behavior for ordinary strings
    return SafeRepresenter.represent_str(dumper, data)

_PjkDumper.add_representer(str, _represent_str)
# --------------------------------------------------------------------------

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
                        yaml.dump(
                            record,
                            sys.stdout,
                            Dumper=_PjkDumper,     # <-- use the custom dumper
                            sort_keys=False,
                            explicit_start=True,
                            allow_unicode=True,
                            width=10**9
                        )
                    except BrokenPipeError:
                        break
        except BrokenPipeError:
            pass
