# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.pipes.factory import PipeFactory
from djk.sources.factory import SourceFactory
from djk.sinks.factory import SinkFactory
from djk.parser import ExpressionParser
from djk.base import Usage
from djk.common import pager_stdout

COLOR_CODES = {
        'bold': '\033[1m',
        'underline': '\033[4m',
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'blue': '\033[34m',
        'magenta': '\033[35m',
        'cyan': '\033[36m',
        'gray': '\033[90m',
    }

RESET = '\033[0m'

def do_all_man():
    with pager_stdout():
        for factory in [SourceFactory, PipeFactory, SinkFactory]:
            comp_type = factory.TYPE
            for name in factory.COMPONENTS.keys():
                usage = factory.get_usage(name)
                print_man(name, usage)
                print()

def do_man(name: str):
    if '--all' in name:
        do_all_man()
        return

    # source and sinks have common names so go through multiple times
    printed = False
    for factory in [SourceFactory, PipeFactory, SinkFactory]:
        usage = factory.get_usage(name)
        if usage:
            print_man(name, usage)
            printed = True

    if not printed:
        print(f'unknown: {name}')

def highlight(text: str, value: str, color: str = 'bold') -> str:
    
    style = COLOR_CODES.get(color.lower(), COLOR_CODES['bold'])
    return text.replace(value, f"{style}{value}{RESET}")

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
    expr_str = highlight(expr_str, name, 'bold')

    #print("pjk", " ".join(quote(t) for t in expr_tokens))
    print('pjk', expr_str)

def print_example(expr_tokens: list[str], expect:str, name: str):
    try:
        if not expect: # if no expect, don't run them, just print them
            smart_print(expr_tokens, name)
            print()
            return

        expr_tokens.append(f'expect:{expect}')
        parser = ExpressionParser(expr_tokens)
        sink = parser.parse()
        sink.drain() # make sure the expect is fulfilled

        expr_tokens[-1] = '-' # for printing so you see simple stdout -
        smart_print(expr_tokens, name)
        expr_tokens[-1] = '-@less=false' # no less since man is doing less
        parser = ExpressionParser(expr_tokens)
        sink = parser.parse()
        sink.drain()
        print()

    except ValueError as e:
        raise 'error executing example'

def print_man(name: str, usage: Usage):
    comp_type = usage.get_base_class(as_string=True)
    header = f'{name} is a {comp_type}'
    print(highlight(header, name, 'bold'))

    print()
    print(usage.get_usage_text())

    examples = usage.get_examples()
    if not examples:
        return
    
    print()
    print('examples:')
    print()

    for expr_tokens, expect in usage.get_examples(): # expect in InlineSource format
        print_example(expr_tokens, expect, name)

def do_examples():
    with pager_stdout():
        for factory in [SourceFactory, PipeFactory, SinkFactory]:
            comp_type = factory.TYPE
            for name, comp_class in factory.COMPONENTS.items():

                header = f'{comp_type}:{name}'
                print(highlight(header, comp_type, 'bold'))

                usage = comp_class.usage()
                examples = usage.get_examples()
                if not examples:
                    print(f'{name} needs examples')
                    print()

                for expr_tokens, expect in examples:
                    print_example(expr_tokens, expect, name)
