# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from typing import Iterable
from pjk.base import Source

class SourceListSource(Source):
    def __init__(self, source_iter: Iterable[Source]):
        self.sources = iter(source_iter)
        self.current = None

    def __iter__(self):
        while True:
            if self.current is None:
                try:
                    self.current = next(self.sources)
                except StopIteration:
                    return  # all sources exhausted

            try:
                yield from self.current
            finally:
                self.current = None
