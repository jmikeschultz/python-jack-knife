# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.components import Source


class DictListSource(Source):
    """Source that yields records from a list of dicts. Used by PjkEngine."""

    def __init__(self, records: list):
        super().__init__(root=None)
        self.records = list(records) if records else []

    def __iter__(self):
        yield from self.records
