# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.parser import ExpressionParser
from pjk.base import Usage, ParsedToken
from pjk.registry import ComponentRegistry
from pjk.main import execute_tokens
def test_parser():
    registry = ComponentRegistry()
    parser = ExpressionParser(registry)
    sink = parser.parse(["[{id: 1}, {id: 2}]", "-"])

    assert next(sink.input) == {'id': 1}
    assert next(sink.input) == {'id': 2}

def test_man():
    execute_tokens(["man", "--all+"])

def test_examples():
    execute_tokens(["examples+"])    
