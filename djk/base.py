from abc import ABC, abstractmethod
from typing import Any, Optional, List, Set
from abc import ABC
from typing import List, Optional

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
        p1s = token.split('@', 1)  # Separate params off
        if len(p1s) > 1:
            param_list = p1s[1].split('@')
            for param in param_list:
                parts = param.split('=')
                value = parts[1] if len(parts) == 2 else None
                self._params[parts[0]] = value

        # args
        p2s = p1s[0].split(':')
        self._main = p2s[0]

        for arg in p2s[1:]: # treat a '' arg as missing and ignore all args after that
            if arg != '':
                self._args.append(arg)
            else:
                break

    @property
    def main(self):
        return self._main
    
    @property
    def whole_token(self):
        return self.token
    
    def num_args(self):
        return len(self._args)
    
    # args are mandatory
    def get_arg(self, arg_no: int):
        return self._args[arg_no] if arg_no < len(self._args) else None

    # params are optional
    def get_params(self) -> dict:
        return self._params
    
class Usage:
    def __init__(self, name: str, desc: str):
        self.name = name
        self.desc = desc
        self.args = {}
        self.params = {}

        self.arg_defs = []
        self.param_usages = {}

    # args and param values default as str
    def def_arg(self, name: str, usage: str, is_num: bool = False, valid_values: Optional[Set[str]] = None):
        self.arg_defs.append((name, usage, is_num, valid_values))

    def def_param(self, name:str, usage: str, is_num: bool = False, valid_values: Optional[Set[str]] = None):
        self.param_usages[name] = (usage, is_num, valid_values)

    def get_arg(self, name: str):
        return self.args.get(name, None)

    def get_param(self, name: str):
        return self.params.get(name, None)
    
    def get_usage_text(self):
        lines = []
        lines.append(self.desc)
        lines.append(f'syntax:')
        lines.append(f'  {self.get_token_syntax()}')
        lines.extend(f"{line}" for line in self.get_arg_param_desc())
        return '\n'.join(lines)

    def get_token_syntax(self):
        token = f'{self.name}'
        for name, usage, is_num, valid_values in self.arg_defs:
            token += f':<{name}>'
        for name, usage, in self.param_usages.items():
            token += f'@{name}=<{name}>'
        return token
    
    def get_arg_param_desc(self):
        notes = []
        notes.append('mandatory args:')
        for name, usage, is_num, valid_values in self.arg_defs:
            notes.append(f'  {name} = {usage}')
        if self.param_usages:
            notes.append('optional params:')
            for name, usage in self.param_usages.items():
                text, is_num, valid_values = usage
                notes.append(f'  {name} = {text}')
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
            
        for name, str_val in ptok.get_params().items():
            usage = self.param_usages.get(name, None)
            if not usage:
                raise TokenError.from_list([f"unknown param: '{name}'.", '', self.get_usage_text()])
            if not str_val:
                raise TokenError.from_list([f"missing value for '{name}' param.", '', self.get_usage_text()])

            text, is_num, valid_values = usage
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
    def __init__(self, name: str, desc: str):
        super().__init__(name=name, desc=desc)
    def bind(self, ptok: ParsedToken):
        return
            
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
    is_format = False

    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component"
        )
    
    @abstractmethod
    def next(self) -> Optional[Any]:
        pass

    def report(self) -> "Report":
        return make_report(self)
    
    def deep_copy(self):
        return None
    
class Pipe(Source):
    deep_copyable: bool = False # default to false
    arity: int = 1
    
    def __init__(self, ptok: ParsedToken, usage: Usage = None):
        self.ptok = ptok
        self.inputs: List[Source] = []

    def set_sources(self, inputs: List[Source]) -> None:
        if len(inputs) != type(self).arity:
            raise ValueError(
                f"{self.__class__.__name__} expects {type(self).arity} input(s), got {len(inputs)}"
            )
        self.inputs = inputs
        self.reset()

    def reset(self):
        pass  # optional hook

    def deep_copy(self) -> Optional["Pipe"]:
        if not self.deep_copyable:
            return None
        if not self.inputs:
            raise RuntimeError(f"{self.__class__.__name__} has no inputs set")

        cloned_inputs = []
        for inp in self.inputs:
            strand = inp.deep_copy()
            if strand is None:
                return None
            cloned_inputs.append(strand)

        clone = self.__class__(arg_string=self.arg_string)
        clone.set_sources(cloned_inputs)
        return clone

class Sink(ABC):
    is_format = False

    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component"
        )
    
    def __init__(self, input_source: Source):
        self.input = input_source

    def drain(self):
        self.process()
        
    @abstractmethod
    def process(self) -> None:
        pass

    def report(self) -> "Report":
        return Report(
            type=self.__class__.__name__,
            inputs=[self.input.report()]
        )

    def render_report(self):
        if getattr(self, "suppress_report", False):
            return
        
        print("\n┌─Execution Report\n│")
        report = self.report()
        render_report_tree(report)
        print()

    def deep_copy(self):
        return None

class Report:
    def __init__(
        self,
        type: str,
        label: Optional[str] = None,
        metrics: Optional[dict] = None,
        inputs: Optional[List["Report"]] = None,
    ):
        self.type = type                        # The component type (e.g. 'JsonSource', 'MoveField')
        self.label = label                      # Optional label (e.g. file path, key field, transform spec)
        self.metrics = metrics or {}            # Dict of counters/statistics
        self.inputs = inputs or []              # List of input Reports

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "label": self.label,
            "metrics": self.metrics,
            "inputs": [r.to_dict() for r in self.inputs]
        }

    def __repr__(self):
        return f"Report(type={self.type}, label={self.label}, metrics={self.metrics}, inputs={len(self.inputs)})"

def make_report(obj, *, label=None, inputs=None):
    metrics = {}

    for k, v in vars(obj).items():
        if k.startswith("_") or callable(v):
            continue

        # Normalize file objects
        if hasattr(v, 'name') and hasattr(v, 'read'):
            v = v.name

        metrics[k] = v

    return Report(
        type=obj.__class__.__name__,
        label=label,
        metrics=metrics,
        inputs=inputs or getattr(obj, "input", None) and [obj.input.report()]
    )

def render_report_tree(report, prefix: str = '', is_last: bool = True):
    connector = '└─' if is_last else '├─'
    label = f" ({report.label})" if report.label else ''
    bracket = format_metrics(report.metrics)
    print(f"{prefix}{connector}{report.type}{label} {bracket}")

    # print metrics under the node, indented further
    if report.metrics:
        metric_prefix = prefix + ('   ' if is_last else '│  ')
        for k, v in report.metrics.items():
            print(f"{metric_prefix}{k}={v}")

    # insert vertical spacer between this node and children
    if report.inputs:
        spacer = prefix + ('│  ' if not is_last else '   ')
        print(spacer)

        for i, child in enumerate(report.inputs):
            render_report_tree(child, prefix=spacer, is_last=(i == len(report.inputs) - 1))

def format_metrics(metrics: dict) -> str:
    if not metrics:
        return ''
    parts = []
    for k, v in metrics.items():
        if isinstance(v, (int, float)):
            parts.append(f"{k}={v}")
    return f"[{', '.join(parts)}]" if parts else ''

def format_metrics2(metrics: dict) -> str:
    if not metrics:
        return ''
    # Example: format a few key fields into [ ... ]
    parts = []
    if 'elapsed_secs' in metrics:
        parts.append(f"{metrics['elapsed_secs']:.1f}s")
    if 'records_sunk' in metrics:
        parts.append(f"{metrics['records_sunk']} recs")
    return f"[{', '.join(parts)}]" if parts else ''

# identity source for sub-pipeline seeding
class IdentitySource(Source):
    def next(self):
        raise RuntimeError("IdentitySource should never be executed")

class ComponentFactory:
    COMPONENTS = {} # name -> component_class
    TYPE = "COMPONENT" # source pipe sink

    @classmethod
    def print_descriptions(cls):
        print(cls.HEADER)
        for name, comp_class in cls.COMPONENTS.items():
            usage = comp_class.usage()

            print(f'  {name:<12} {usage.desc}')

    def create(cls, token: str):
        pass