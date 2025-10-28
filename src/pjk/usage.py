from typing import Optional, Set, List
import os
import yaml

CONFIG_FILE = '~/.pjk/configs.yaml'

class Config:
    def __init__(self):
        self.configs_yaml = os.path.expanduser(CONFIG_FILE)
        self._data = {}
        self._load()
        
    def _load(self):
        if os.path.exists(self.configs_yaml):
            with open(self.configs_yaml, 'r') as f:
                self._data = yaml.safe_load(f) or {}
        else:
            self._data = {}

    def lookup(self, usage: "Usage", param: str):
         # this should be advertised as a well-known requirement: usage must define a 'instance' arg
        instance = usage.get_arg("instance")
        if not instance:
            raise TokenError(f"'instance' arg must be defined when using configs in {CONFIG_FILE}")

        component_class = usage.get_component_class()
        class_name = component_class.__name__

         # name, type, default
        tuples_dict = usage.get_config_tuples()
        type_default = tuples_dict.get(param)
        if not type_default:
            raise TokenError(f"{class_name} does not define '{param}' in config_tuples")

        (param_type, param_default) = type_default

        instance_key = f'{class_name}-{instance}'
        entry = self._data.get(instance_key, None)
        if not entry:
            raise TokenError(
                f"{CONFIG_FILE} does not contain entry for '{instance_key}' with required params."
            )
        
        _alias = entry.get('_alias', None) # _alias must = another entry instance_key
        if _alias:
            entry = self._data.get(_alias, None)
            if not entry:
                raise TokenError(
                    f"'{instance_key}:_alias' in {CONFIG_FILE} points to a non-existent entry: '{_alias}'."
                )    
        
        raw = entry.get(param, param_default)

        if not raw:
            return None

        if param_type == str:
            return raw
        
        if param_type == bool:
            if type(raw) == bool:
                return raw
            return raw.lower() != 'false'
        
        if param_type == float:
            if type(raw) == float:
                return raw
            return float(raw)
        
        if param_type == int:
            if type(raw) == int:
                return raw
            return int(raw)
        
        else:
            raise(f'unsupported type: {param_type}')

# singleton
configs = Config()

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
    
class Usage:
    def __init__(self, name: str, desc: str, component_class: type):
        self.name = name
        self.desc = desc
        self.comp_class = component_class
        self.args = {}
        self.params = {}
        self.shape = None
        self.syntax = None

        self.arg_defs = []
        self.param_usages = {}
        self.examples = []

        self.config_tuples = [] # name, type, default

    def get_component_class(self):
        return self.comp_class

    def def_shape(self, shape: str):
        self.shape = shape

    def def_config_tuples(self, tuples: list):
        self.config_tuples = tuples

    def get_config_tuples(self) -> dict:
        return {n: (t, d) for n, t, d in self.config_tuples}

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
        if self.syntax:
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

    def get_config(self, name: str):
        return configs.lookup(self, name)

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
    
