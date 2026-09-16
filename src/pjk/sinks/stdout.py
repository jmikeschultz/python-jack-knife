# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import sys
import yaml
from yaml.representer import SafeRepresenter  # kept for compatibility
from pjk.components import Sink, Source
from pjk.usage import ParsedToken, Usage
from pjk.common import pager_stdout


MORE_KEY = '_more'


def _is_flat_map(d: dict) -> bool:
    """
    A "flat map" is a key-bag (id→score, id→list, …): no value is itself a dict.
    A dict that embeds other dicts is treated as a struct instead.
    """
    return not any(isinstance(v, dict) for v in d.values())


def shorten_value(obj, n: int, *, role: str = 'record'):
    """
    Display-only overview transform (never mutates input; pipeline records stay full).

    Roles
    -----
    record — structured object (root record, list element, or nested struct).
             Keeps every key; shortens each value.
    map    — flat key-bag (no nested-dict values). Keeps first n keys; sets
             ``_more: K`` for omitted keys.

    A nested dict offered as role=map is promoted to record when it is not flat
    (so structs like sourcedAds keep all fields; score maps still get capped).

    Lists keep the first n elements (each element is a record) and append
    ``{_more: K}`` when truncated.
    """
    if isinstance(obj, list):
        omitted = len(obj) - n
        out = [shorten_value(x, n, role='record') for x in obj[: max(n, 0)]]
        if omitted > 0:
            out.append({MORE_KEY: omitted})
        return out

    if isinstance(obj, dict):
        # Nested dicts that contain other dicts are structs, not key-bags.
        if role == 'map' and not _is_flat_map(obj):
            role = 'record'

        if role == 'record':
            return {
                k: shorten_value(
                    v,
                    n,
                    role='map' if isinstance(v, dict) else 'record',
                )
                for k, v in obj.items()
            }

        # flat map: cap keys
        items = list(obj.items())
        omitted = len(items) - n
        out = {}
        for k, v in items[: max(n, 0)]:
            out[k] = shorten_value(v, n, role='record')
        if omitted > 0:
            out[MORE_KEY] = omitted
        return out

    return obj


class StdoutSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='-',
            desc=(
                "display records in yaml or txt format to stdout through less.\n"
                "Optional @shorten=N is display-only: caps list lengths and flat-map "
                "key counts for overview without changing pipeline records."
            ),
            component_class=cls
        )
        usage.def_param('less', usage='use less to display', valid_values=['true', 'false'], default='true')
        usage.def_param('format', usage='output format', valid_values=['yaml', 'txt'], default='yaml')
        usage.def_param(
            'shorten',
            usage=(
                'display only; does not alter pipeline data. Recursively:\n'
                '  - lists → first N elements (list items keep all their keys)\n'
                '  - flat maps (no nested-dict values) → first N keys\n'
                '  - structs (dicts that embed other dicts) → keep all keys\n'
                'Truncation adds {_more: K} (list element or map field).\n'
                'Example: pjk out.json grab:pipeline.tasks -@shorten=5'
            ),
            is_num=True,
            default=None,
        )
        usage.def_example(["{hello:'world!'}"], "{hello:'world!'}")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.use_pager = True if usage.get_param('less') is None else usage.get_param('less') == 'true'
        self.output_format = usage.get_param('format') or 'yaml'
        shorten = usage.get_param('shorten')
        self.shorten_n = int(shorten) if shorten is not None else None

    def _for_display(self, record):
        if self.shorten_n is None:
            return record
        return shorten_value(record, self.shorten_n, role='record')

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
                    self._for_display(record),
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
                record = self._for_display(record)
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
