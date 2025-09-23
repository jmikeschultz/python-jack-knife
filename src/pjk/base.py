# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from abc import ABC, abstractmethod
from typing import Any, Optional, List, Set

class TokenError(ValueError):
    @classmethod
    def from_list(cls, lines: List[str]):
        text = '\n'.join(lines)
        return TokenError(text)

    def __init__(self, text: str):
        super().__init__(text)
        self.text = text

    def get_text(self):
        return self.text
    
class UsageError(ValueError):
    def __init__(self, message: str,
                 tokens: List[str] = None,
                 token_no: int = 0,
                 token_error: TokenError = None):
        super().__init__(message)
        self.message = message
        self.tokens = tokens
        self.token_no = token_no
        self.token_error = token_error

    def __str__(self):
        lines = []
        token_copies = [self._quote(t) for t in self.tokens]
        lines.append('pjk ' + ' '.join(token_copies))
        lines.append(self._get_underline(token_copies))
        lines.append(self.message)
        lines.append('')
        lines.append(self.token_error.get_text())
        return '\n'.join(lines)
    
    # quote json inline 
    def _quote(self, token):
        if token.startswith('[') or token.startswith('{'):
            return '"' + token + '"'
        else:
            return token

    def _get_underline(self, tokens: List, marker='^') -> str:
        offset = 4 + sum(len(t) + 1 for t in tokens[:self.token_no])  # +1 for space, 4 for pjk
        underline = ' ' * offset + marker * len(tokens[self.token_no])
        return underline

class ParsedToken:
    def __init__(self, token: str):
        self.token = token
        self._params = {}
        self._args = []
        at_parts = token.split('@', 1)  # Separate params off
        if len(at_parts) > 1:
            param_list = at_parts[1].split('@')
            for param in param_list:
                parts = param.split('=')
                value = parts[1] if len(parts) == 2 else None
                self._params[parts[0]] = value

        self._all_but_params = at_parts[0]

        # args
        colon_parts = at_parts[0].split(':')
        self._pre_colon = colon_parts[0]

        for arg in colon_parts[1:]: # treat a '' arg as missing and ignore all args after that
            if arg != '':
                self._args.append(arg)
            else:
                break

    @property
    def pre_colon(self):
        return self._pre_colon
    
    @property
    def whole_token(self):
        return self.token
    
    @property # avoid colon parsing
    def all_but_params(self):
        return self._all_but_params
    
    def num_args(self):
        return len(self._args)
    
    # args are mandatory
    def get_arg(self, arg_no: int):
        return self._args[arg_no] if arg_no < len(self._args) else None

    # params are optional
    def get_params(self) -> dict:
        return self._params
    
class Usage:
    def __init__(self, name: str, desc: str, component_class: type):
        self.name = name
        self.desc = desc
        self.comp_class = component_class
        self.args = {}
        self.params = {}
        self.syntax = None

        self.arg_defs = []
        self.param_usages = {}
        self.examples = []

    def get_component_class(self):
        return self.comp_class
    
    def get_base_class(self, as_string: bool = False):
        if issubclass(self.comp_class, Sink):
            return 'sink' if as_string else Sink
        elif issubclass(self.comp_class, Pipe):
            return 'pipe' if as_string else Pipe
        elif issubclass(self.comp_class, Source):
            return 'source' if as_string else Source
        raise 'improper class'

    # args and param values default as str
    def def_arg(self, name: str, usage: str, is_num: bool = False, valid_values: Optional[Set[str]] = None):
        self.arg_defs.append((name, usage, is_num, valid_values))

    def def_param(self, name:str, usage: str, is_num: bool = False, valid_values: Optional[Set[str]] = None, default:str = None):
        self.param_usages[name] = (usage, is_num, valid_values, default)
        if default:
            self.params[name] = self._get_val(default, is_num, valid_values)

    def def_example(self, expr_tokens:list[str], expect:str):
        self.examples.append((expr_tokens, expect))

    def def_syntax(self, syntax: str):
        self.syntax = syntax

    def get_examples(self):
        return self.examples

    def get_arg(self, name: str):
        return self.args.get(name, None)
    
    def get_param(self, name: str):
        return self.params.get(name)
    
    def get_usage_text(self):
        lines = []
        lines.append(self.desc)

        syntax_str = self.get_token_syntax() # might be ''
        if not syntax_str:
            return '\n'.join(lines)
        
        lines.append('')
        lines.append(f'syntax:')
        lines.append(f'  {self.get_token_syntax()}')
        lines.extend(f"{line}" for line in self.get_arg_param_desc())
        return '\n'.join(lines)

    def get_token_syntax(self):
        if not self.syntax:
            return self.syntax # else piece it together

        token = f'{self.name}'
        for name, usage, is_num, valid_values in self.arg_defs:
            token += f':<{name}>'

        for name, (usage, is_num, valid_values, default) in self.param_usages.items():
            value_display = name
            if valid_values:
                value_display  = '|'.join(list(valid_values))
            token += f'@{name}=<{value_display}>'
        return token
    
    def get_arg_param_desc(self):
        notes = []
        if self.arg_defs:
            notes.append('mandatory args:')
            for name, usage, is_num, valid_values in self.arg_defs:
                notes.append(f'  {name} = {usage}')

        if self.param_usages:
            notes.append('optional params:')
            for name, usage in self.param_usages.items():
                text, is_num, valid_values, default = usage
                notes.append(f'  {name} = {text} (default={default})')
        return notes

    def bind(self, ptok: ParsedToken):
        if ptok.num_args() > len(self.arg_defs):
            extra = []
            for i in range(len(self.arg_defs), ptok.num_args()):
                name = ptok.get_arg(i)
                extra.append(name)

            raise TokenError.from_list([f"extra arg{'s' if len(extra) > 1 else ''}: {','.join(extra)}.", 
                                        '', self.get_usage_text()])
        
        if ptok.num_args() < len(self.arg_defs):
            missing = []
            for i in range(ptok.num_args(), len(self.arg_defs)):
                name, usage, is_num, valid_values = self.arg_defs[i]
                missing.append(name)

            raise TokenError.from_list([f"missing arg{'s' if len(missing) > 1 else ''}: {','.join(missing)}.", 
                                        '', self.get_usage_text()])

        for i, adef in enumerate(self.arg_defs):
            name, usage, is_num, valid_values = adef

            try:
                val_str = ptok.get_arg(i)
                self.args[name] = self._get_val(val_str, is_num, valid_values)
            except (ValueError, TypeError) as e:
                raise TokenError.from_list([f"wrong value for '{name}' arg.", '', self.get_usage_text()])

        self.bind_params(ptok)
        
    def bind_params(self, ptok: ParsedToken):
        for name, str_val in ptok.get_params().items():
            usage = self.param_usages.get(name, None)
            if not usage:
                raise TokenError.from_list([f"unknown param: '{name}'.", '', self.get_usage_text()])
            if not str_val:
                raise TokenError.from_list([f"missing value for '{name}' param.", '', self.get_usage_text()])

            text, is_num, valid_values, default = usage
            try:
                self.params[name] = self._get_val(str_val, is_num, valid_values)
            except (ValueError, TypeError) as e:
                raise TokenError.from_list([f"wrong value type for '{name}' param.", '', self.get_usage_text()])

    def _get_val(self, val_str: str, is_num: bool, valid_values: Optional[Set[str]] = None):
        if not val_str:
            raise ValueError('missing value')
        if not is_num: # is string
            if valid_values is None: # no constraints
                return val_str
            if not val_str in valid_values:
                raise ValueError(f'illegal value: {val_str}')
            return val_str
            
        else: # is_num
            try:
                return int(val_str)
            except ValueError as e: # coud be a float that errors, but is ok
                return float(val_str)

# until all usages are implemented a default that doesn't bind
# they continue to use ParsedToken ptok
class NoBindUsage(Usage):
    def __init__(self, name: str, desc: str, component_class: type):
        super().__init__(name=name, desc=desc, component_class=component_class)
    def bind(self, ptok: ParsedToken):
        return

# mixin
class KeyedSource(ABC):
    @classmethod
    def usage(cls):
        return Usage(
            name=cls.__name__,
            desc=f"{cls.__name__} component"
        )
    
    @abstractmethod
    def lookup(self, left_rec) -> Optional[dict]:
        """Return the record associated with the given key, or None."""
        pass

    def get_unlookedup_records(self) -> List[Any]:
        # for outer join
        pass

    def deep_copy(self):
        return None

class Source(ABC):
    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component",
            component_class=cls
        )

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError("__iter__ must be implemented by subclasses")

    def __next__(self):
        # lazily create an internal iterator the first time next() is called
        if not hasattr(self, "_iter"):
            self._iter = iter(self)
        return next(self._iter)

    def deep_copy(self):
        return None  # Default: not copyable unless overridden

    
class Pipe(Source):
    deep_copyable: bool = False # default to false
    arity: int = 1
    
    def __init__(self, ptok: ParsedToken, usage: Usage = None):
        self.ptok = ptok
        self.usage = usage
        self.left = None  # left source for convience
        self.right = None # right source for convience
        self.inputs: List[Source] = []

    def add_source(self, source: Source) -> None:
        self.inputs.append(source)
        # first two are assigned left, right
        if self.left is None:
            self.left = source
        elif self.right is None:
            self.right = self.left
            self.left = source

    def reset(self):
        pass  # optional hook

    def deep_copy(self) -> Optional["Pipe"]:
        if not self.deep_copyable:
            return None
        if not self.inputs:
            raise RuntimeError(f"{self.__class__.__name__} has no inputs set")

        clone = self.__class__(self.ptok, self.__class__.usage())

        for input in self.inputs:
            strand = input.deep_copy()
            if strand is None:
                return None
            clone.add_source(strand)

        return clone

class DeepCopyPipe(Pipe):
    def deep_copy(self):
        """
        Generic deep_copy: clone left source, re-instantiate
        this pipe class with the same ptok/usage, and attach.
        """
        source_clone = self.left.deep_copy()
        if not source_clone:
            return None

        # re-instantiate using the actual subclass
        pipe = type(self)(self.ptok, self.usage)
        pipe.add_source(source_clone)
        return pipe

class Sink(ABC):
    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component",
            component_class=cls
        )
    
    def __init__(self, ptok: ParsedToken, usage: Usage = None):
        self.ptok = ptok
        self.usage = usage

    def drain(self):
        self.process()
        self.close()

    # optional
    def close(self):
        pass

    def print_info(self):
        pass

    def add_source(self, source: Source) -> None:
        self.input = source
        
    @abstractmethod
    def process(self) -> None:
        pass

    def deep_copy(self):
        return None

# identity source for sub-pipeline seeding
class IdentitySource(Source):
    def next(self):
        raise RuntimeError("IdentitySource should never be executed")

