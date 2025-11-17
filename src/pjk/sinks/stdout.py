# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
from yaml.representer import SafeRepresenter  # kept for compatibility
from pjk.components import Sink, Source
from pjk.usage import ParsedToken, Usage
from pjk.common import pager_stdout


class StdoutSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='-',
            desc='display records in yaml or txt format to stdout through less',
            component_class=cls
        )
        usage.def_param('less', usage='use less to display', valid_values=['true', 'false'], default='true')
        usage.def_param('format', usage='output format', valid_values=['yaml', 'txt'], default='yaml')
        usage.def_example(["{hello:'world!'}"], "{hello:'world!'}")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.use_pager = True if usage.get_param('less') is None else usage.get_param('less') == 'true'
        self.output_format = usage.get_param('format') or 'yaml'

    def _sanitize_scalar(self, v) -> str:
        if v is None:
            s = ''
        elif isinstance(v, (list, dict)):
            # single-line YAML to preserve structure compactly
            s = yaml.safe_dump(v, sort_keys=False, allow_unicode=True).strip()
        else:
            s = str(v)
        return s.replace('\r', ' ').replace('\n', ' ')

    def _process_yaml(self):
        for record in self.input:
            try:
                yaml.dump(
                    record,
                    sys.stdout,
                    sort_keys=False,
                    explicit_start=True,  # '---' before each record
                    allow_unicode=True,
                    width=10**9,
                )
            except BrokenPipeError:
                break

    def _process_txt(self):
        for record in self.input:
            try:
                # record delimiter
                sys.stdout.write('---\n')
                if isinstance(record, dict):
                    for k, v in record.items():
                        key = str(k)
                        val = self._sanitize_scalar(v)
                        sys.stdout.write(f'{key}: {val}\n')
                else:
                    val = self._sanitize_scalar(record)
                    sys.stdout.write(f'value: {val}\n')
            except BrokenPipeError:
                break

    def process(self) -> None:
        try:
            with pager_stdout(self.use_pager):
                if self.output_format == 'txt':
                    self._process_txt()
                else:
                    self._process_yaml()
        except BrokenPipeError:
            pass
