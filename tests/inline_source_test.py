# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
import pytest
from pjk.sources.inline_source import InlineSource

def test_source_list_basic():
    src = InlineSource('[{foo: 1}, {foo: 2}]')

    assert next(src) == {"foo": 1}
    assert next(src) == {"foo": 2}
    with pytest.raises(StopIteration):
        next(src)
