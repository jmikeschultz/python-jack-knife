# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.base import Source, ParsedToken

class MySource(Source):
    def __init__(self, ptok: ParsedToken):
        self.done = False

    def next(self):
        if not self.done:
            self.done = True
            return {"hello": "world"}
        return None
