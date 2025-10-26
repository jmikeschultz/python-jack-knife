# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.components import Sink
from pjk.usage import ParsedToken, Usage
from importlib.resources import files
from pathlib import Path
from typing import Dict
import re
import sys

# Explicit mapping of component type -> template resource path
_TEMPLATES: Dict[str, str] = {
    "source": "resources/source.tmpl",
    "pipe":   "resources/pipe.tmpl",
    "sink":   "resources/sink.tmpl",
}

def _to_camel(name: str) -> str:
    """
    Convert a name like 'foo', 'foo_bar', 'foo-bar', 'foo bar'
    into 'Foo', 'FooBar', ... for class names.
    """
    parts = re.split(r"[_\-\s]+", name.strip())
    parts = [p for p in parts if p]
    return "".join(p[:1].upper() + p[1:] for p in parts)


def _sanitize_basename(name: str) -> str:
    """
    Sanitize to a filesystem-friendly, pythonic base filename (snake-ish).
    Keeps letters, numbers, underscore; collapses others to '_'.
    """
    base = re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_")
    if not base:
        raise ValueError("name produces empty basename after sanitization")
    return base


class CreateSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='create',
            desc=(
                "Write a bare bones source, pipe or sink python file.\n"
                "Requires input record of the form: \"{<type>: '<name>'}\". E.g. \"{pipe: 'mypipe'}\"\n"
                "User components can be used inline or deposited in ~/.pjk/plugins"
            ),
            component_class=cls
        )
        usage.def_param(name='overwrite', usage='whether to overwrite output file', valid_values={'true', 'false'}, default='false')

        usage.def_example(
            expr_tokens=["{pipe: 'mypipe'}", 'create'],
            expect=None
        )
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.overwrite = usage.get_param('overwrite').lower() == 'true'

    def process(self):
        record = next(self.input)
        ctype, cname = next(iter(record.items()))

        if ctype not in _TEMPLATES:
            raise Exception(f"create: unsupported type '{ctype}'; expected one of {list(_TEMPLATES.keys())}")

        # ----- Resolve names/paths
        class_name = _to_camel(f'{cname}_{ctype}')
        print(class_name)
        base_filename = _sanitize_basename(cname)
        out_filename = f"{base_filename}_{ctype}.py"
        out_path = Path.cwd() / out_filename

        # ----- Load template text safely from the installed package
        template_rel = _TEMPLATES[ctype]
        try:
            template_text = files("pjk").joinpath(template_rel).read_text(encoding="utf-8")
        except FileNotFoundError as e:
            raise Exception(f"create: template '{template_rel}' not found in package 'pjk'") from e

        # ----- Render substitutions
        rendered = (
            template_text
            .replace("__NAME__", cname)
            .replace("__CLASS__", class_name)
        )
        if not rendered.endswith("\n"):
            rendered += "\n"

        # ----- Write output (with overwrite protection)
        if out_path.exists() and not self.overwrite:
            raise Exception(f"create: {out_path} already exists (set param overwrite=true to replace)")

        out_path.write_text(rendered, encoding="utf-8")

    def close(self):
        pass

    def deep_copy(self):
        return None

    def reset(self):
        pass
