# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.pipes.factory import PipeFactory
from djk.sources.factory import SourceFactory
from djk.sinks.factory import SinkFactory
from djk.parser import ExpressionParser
from djk.base import Usage
import shlex

def do_man(name: str):
    for factory in [SourceFactory, PipeFactory, SinkFactory]:
        usage = factory.get_usage(name)
        if usage:
            print_man(name, usage)
            return

    print(f'unknown: {name}')

def smart_print(expr_tokens: list[str]):
    import re
    SAFE_UNQUOTED_RE = re.compile(r"^[a-zA-Z0-9._/:=+-]+$")

    def quote(token: str) -> str:
        if SAFE_UNQUOTED_RE.fullmatch(token):
            return token
        elif "'" not in token:
            return f"'{token}'"
        elif '"' not in token:
            return f'"{token}"'
        else:
            return '"' + token.replace('"', '\\"') + '"'

    print("pjk", " ".join(quote(t) for t in expr_tokens))

def print_example(expr_tokens: list[str], expect:str):
    try:
        expr_tokens.append(f'expect:{expect}')

        parser = ExpressionParser(expr_tokens)
        sink = parser.parse()
        sink.drain() # make sure the expect is fulfilled

        expr_tokens[-1] = '-' # change sink to expression out
        smart_print(expr_tokens)
        parser = ExpressionParser(expr_tokens)
        sink = parser.parse()
        sink.drain()
        print()

    except ValueError as e:
        raise 'error executing example'

def print_man(name: str, usage: Usage):
    print(name)
    print()
    print(usage.get_usage_text())

    examples = usage.get_examples()
    if not examples:
        return
    
    print()
    print('examples:')
    print()

    for expr_tokens, expect in usage.get_examples(): # expect in InlineSource format
        print_example(expr_tokens, expect)

def do_examples():
    for factory in [SourceFactory, PipeFactory, SinkFactory]:
        for name, comp_class in factory.COMPONENTS.items():
            usage = comp_class.usage()
            examples = usage.get_examples()
            if not examples:
                print(f'{name} needs examples')
                print()

            for expr_tokens, expect in examples:
                print_example(expr_tokens, expect)
            #print(name, comp_class, usage, examples)

        #usage = factory.get_usage(name)
        #if usage:
