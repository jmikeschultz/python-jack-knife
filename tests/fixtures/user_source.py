# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Source, ParsedToken, Usage

class MySource(Source):
    def __init__(self, ptok: ParsedToken, usage: Usage):
        self.done = False

    def __iter__(self):
        yield {"hello": "world"}
