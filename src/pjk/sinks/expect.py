# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Source, Sink, ParsedToken, Usage
from pjk.sources.inline_source import InlineSource
import sys

class ExpectSink(Sink):
    # NOTE: ExpectSink intentionally does NOT use Usage due to raw JSON argument parsing
    # e.g., expect:'[{a:1},{a:2}]' must preserve the entire post-colon string unparsed

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.inline = ptok.whole_token.split(':', 1)[-1]
        self.expect_source = InlineSource(self.inline)
        self._expect_iter = iter(self.expect_source)

    def print_info(self):
        command = ' '.join(sys.argv[1:-1])  # omit 'pjk' and 'expect'
        print(f'{command} ==> OK!\n') # only prints on success

    def process(self) -> None:
        command = ' '.join(sys.argv[1:-1])  # omit 'pjk' and 'expect'

        for test_rec in self.input:
            try:
                expect_rec = next(self._expect_iter)
            except StopIteration:
                raise ValueError(
                    f"expect failure: {command}\n"
                    f"expected_record:None\n"
                    f"got_record:{test_rec}\n"
                    f"entire_expected:{self.inline}"
                )

            if test_rec != expect_rec:
                raise ValueError(
                    f"expect failure: {command}\n"
                    f"expected_record:{expect_rec}\n"
                    f"got_record:{test_rec}\n"
                    f"entire_expected:{self.inline}"
                )

        try:
            expect_rec = next(self._expect_iter)
            raise ValueError(
                f"expect failure: {command}\n"
                f"expected_record:{expect_rec}\n"
                f"got_record:None\n"
                f"entire_expected:{self.inline}"
            )
        except StopIteration:
            pass
