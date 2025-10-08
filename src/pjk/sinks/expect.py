# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Source, Sink, ParsedToken, Usage
from pjk.sources.inline_source import InlineSource
import sys
import json
from collections import Counter

class ExpectSink(Sink):
    # NOTE: ExpectSink intentionally does NOT use Usage due to raw JSON argument parsing
    # e.g., expect:'[{a:1},{a:2}]' must preserve the entire post-colon string unparsed

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.inline = ptok.whole_token.split(':', 1)[-1]
        self.expect_source = InlineSource(self.inline)

    @staticmethod
    def _norm(rec) -> str:
        # Canonicalize for order-insensitive equality and hashing.
        # Using compact separators keeps diffs tidy.
        return json.dumps(rec, sort_keys=True, separators=(',', ':'))

    @staticmethod
    def _pretty(rec_str: str):
        # Turn normalized JSON back into pretty JSON for error messages.
        try:
            return json.dumps(json.loads(rec_str), sort_keys=True)
        except Exception:
            return rec_str

    def process(self) -> None:
        command = ' '.join(sys.argv[1:-1])  # omit 'pjk' and 'expect'

        # Collect expected (from inline source) and actual (from upstream)
        expected = list(self.expect_source)
        actual = list(self.input)

        exp_ctr = Counter(self._norm(r) for r in expected)
        act_ctr = Counter(self._norm(r) for r in actual)

        if exp_ctr == act_ctr:
            return  # success

        # Build a clear diff with counts
        missing = []
        unexpected = []

        # Records expected but not (sufficiently) present
        for rec, exp_n in exp_ctr.items():
            act_n = act_ctr.get(rec, 0)
            if act_n < exp_n:
                missing.append((rec, exp_n - act_n))

        # Records present but not expected (or too many)
        for rec, act_n in act_ctr.items():
            exp_n = exp_ctr.get(rec, 0)
            if act_n > exp_n:
                unexpected.append((rec, act_n - exp_n))

        # Sort for stable, readable output
        missing.sort(key=lambda x: x[0])
        unexpected.sort(key=lambda x: x[0])

        # Format sections
        missing_str = (
            "[\n  " + ",\n  ".join(
                f"{{'record': {self._pretty(r)}, 'count_diff': {n}}}" for r, n in missing
            ) + "\n]"
        ) if missing else "[]"

        unexpected_str = (
            "[\n  " + ",\n  ".join(
                f"{{'record': {self._pretty(r)}, 'count_diff': {n}}}" for r, n in unexpected
            ) + "\n]"
        ) if unexpected else "[]"

        entire_expected_str = json.dumps(expected, sort_keys=True)

        raise ValueError(
            "expect failure (order-insensitive): {cmd}\n"
            "missing_records:{missing}\n"
            "unexpected_records:{unexpected}\n"
            "entire_expected:{entire_expected}"
            .format(
                cmd=command,
                missing=missing_str,
                unexpected=unexpected_str,
                entire_expected=entire_expected_str,
            )
        )
