# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.pipes.factory import PipeFactory
from pjk.sources.factory import SourceFactory
from pjk.sinks.factory import SinkFactory
from pjk.parser import ExpressionParser
from pjk.base import Usage
from pjk.registry import ComponentRegistry
from pjk.common import pager_stdout, highlight
from contextlib import nullcontext

def smart_print(expr_tokens: list[str], name: str):
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

    expr_str = ' '.join(quote(t) for t in expr_tokens)
    expr_str = highlight(expr_str, 'bold', name)

    #print("pjk", " ".join(quote(t) for t in expr_tokens))
    print('pjk', expr_str)

def do_man(name: str, registry: ComponentRegistry):
    no_pager = name.endswith('+')
    if '--all' in name:
        do_all_man(registry, no_pager=no_pager)
        return

    # source and sinks have common names so go through multiple times
    printed = False
    for factory in registry.get_factories():
        usage = factory.get_usage(name)
        if usage:
            print_man(registry, name, usage)
            printed = True

    if not printed:
        print(f'unknown: {name}')

def do_all_man(registry: ComponentRegistry, no_pager: bool = True):
    cm = nullcontext() if no_pager else pager_stdout()
    with cm:
        for factory in registry.get_factories():
            comp_type = factory.get_comp_type_name()
            for name in factory.components.keys():
                usage = factory.get_usage(name)
                print_man(registry, name, usage)
                print()

def print_man(registry: ComponentRegistry, name: str, usage: Usage):
    comp_type = usage.get_base_class(as_string=True)
    header = f'{name} is a {comp_type}'
    print('===================================')
    print('        ', highlight(header, 'bold', name))
    print('===================================')

    print()
    print(usage.get_usage_text())

    examples = usage.get_examples()
    if not examples:
        return
    
    print()
    print('examples:')
    print()

    for expr_tokens, expect in usage.get_examples(): # expect in InlineSource format
        print_example(registry, expr_tokens, expect, name)

def do_examples(token:str, registry: ComponentRegistry):
    no_pager = token.endswith('+')
    cm = nullcontext() if no_pager else pager_stdout()
    with cm:
        for factory in registry.get_factories():
            comp_type = factory.get_comp_type_name()
            for name, comp_class in factory.components.items():
                usage = comp_class.usage()

                comp_type = usage.get_base_class(as_string=True)
                header = f'{name} is a {comp_type}'
                print('===================================')
                print('        ', highlight(header, 'bold', name))
                print('===================================')

                examples = usage.get_examples()
                if not examples:
                    print(f'{name} needs examples')
                    print()

                for expr_tokens, expect in examples:
                    print_example(registry, expr_tokens, expect, name)

def print_example(registry: ComponentRegistry, expr_tokens: list[str], expect:str, name: str):
    try:
        if not expect: # if no expect, don't run them, just print them
            smart_print(expr_tokens, name)
            print()
            return

        expr_tokens.append(f'expect:{expect}')
        parser = ExpressionParser(registry)
        sink = parser.parse(expr_tokens)
        sink.drain() # make sure the expect is fulfilled

        expr_tokens[-1] = '-' # for printing so you see simple stdout -
        smart_print(expr_tokens, name)
        expr_tokens[-1] = '-@less=false' # no less since man is doing less
        parser = ExpressionParser(registry)
        sink = parser.parse(expr_tokens)
        sink.drain()
        print()

    except ValueError as e:
        raise 'error executing example'