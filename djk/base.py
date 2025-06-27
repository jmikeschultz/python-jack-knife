from abc import ABC, abstractmethod
from typing import Any, Optional, List

from abc import ABC
from typing import List, Optional

class KeyedSource(ABC):
    @abstractmethod
    def get_keyed_field(self) -> str:
        """Return the field name this source is keyed on."""
        pass

    @abstractmethod
    def get_record(self, key) -> Optional[dict]:
        """Return the record associated with the given key, or None."""
        pass

    def deep_copy(self):
        return None

class Source(ABC):
    @abstractmethod
    def next(self) -> Optional[Any]:
        pass

    def report(self) -> "Report":
        return make_report(self)
    
    def deep_copy():
        return None
    
class Pipe(Source):
    deep_copyable = True
    arity: int = 1

    def __init__(self, arg_string: str = ""):
        self.arg_string = arg_string
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

class PipeSyntaxError(ValueError):
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}

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
