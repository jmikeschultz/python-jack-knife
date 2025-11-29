# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.parser import ExpressionParser, MACRO_PREFIX, MACROS_FILE, read_macros
from pjk.components import Source, Pipe, Sink
from pjk.usage import Usage, CONFIG_FILE
from pjk.registry import ComponentRegistry
from pjk.common import pager_stdout, highlight, ComponentOrigin
from contextlib import nullcontext
import yaml
import sys
from pathlib import Path

def get_base_class(usage: Usage, as_string: bool = False):
        if issubclass(usage.comp_class, Sink):
            return 'sink' if as_string else Sink
        elif issubclass(usage.comp_class, Pipe):
            return 'pipe' if as_string else Pipe
        elif issubclass(usage.comp_class, Source):
            return 'source' if as_string else Source
        raise 'improper class'

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
        comp_class = factory.get_component_class(name)
        if comp_class:
            print_man(registry, name, comp_class.usage())
            printed = True

    if not printed:
        print(f'unknown: {name}')

def do_all_man(registry: ComponentRegistry, no_pager: bool = True):
    cm = nullcontext() if no_pager else pager_stdout()
    with cm:
        for factory in registry.get_factories():
            component_dict = factory.get_components([ComponentOrigin.CORE, ComponentOrigin.EXTERNAL, ComponentOrigin.USER], is_integration=None)
            for name, comp_class in component_dict.items():
                print_man(registry, name, comp_class.usage())
                print()

def print_man(registry: ComponentRegistry, name: str, usage: Usage):
    comp_type = get_base_class(usage, as_string=True)
    header = f'{name} is a {comp_type}'

    print('===================================')
    print('        ', highlight(header, 'bold', name))
    print('===================================')

    print(usage.get_usage_text())

    examples = usage.get_examples()
    if not examples:
        return
    
    print()
    print('examples:')
    print()

    for expr_tokens, expect in usage.get_examples(): # expect in InlineSource format
        print_example(registry, expr_tokens, expect, name)

def display_configs():
    path = Path(CONFIG_FILE).expanduser()

    with pager_stdout():
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("Top-level YAML must be a mapping of records")
        
        print(f'Component configs defined in {CONFIG_FILE}')
        print()
        for name, body_dict in data.items():
            print('=========================================')
            print('   ', highlight(name, 'bold', name))
            print('=========================================')

            if 'password' in body_dict:
                body_dict['password'] = '*************'

            try:
                yaml.dump(
                    body_dict,
                    sys.stdout,
                    sort_keys=False,
                    explicit_start=False,
                    allow_unicode=True,
                    width=10**9,
                )
            except BrokenPipeError:
                break
            print()

def display_macros():
    macros = read_macros()

    with pager_stdout():
        print(f"Macros defined in '{MACROS_FILE}'")

        print(f"Usage: pjk [...] {MACRO_PREFIX}:<macro_name> [...]")
        print()
        for name, value in macros.items():
            print(f'{name}: {value}')
            print()

def do_examples(token:str, registry: ComponentRegistry):
    no_pager = token.endswith('+')
    cm = nullcontext() if no_pager else pager_stdout()
    with cm:
        for factory in registry.get_factories():
            component_dict = factory.get_components([ComponentOrigin.CORE, ComponentOrigin.EXTERNAL, ComponentOrigin.USER], is_integration=None)
            for name, comp_class in component_dict.items():
                usage = comp_class.usage()

                comp_type = get_base_class(usage, as_string=True)
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