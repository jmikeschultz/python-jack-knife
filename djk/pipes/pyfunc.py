import importlib.util
from typing import Optional
from djk.base import Pipe, PipeSyntaxError, Source

class PythonFunctionPipe(Pipe):
    def __init__(self, script_path: str, arg_string: str = ""):
        super().__init__(arg_string)

        spec = importlib.util.spec_from_file_location("user_pipe", script_path)
        if spec is None or spec.loader is None:
            raise PipeSyntaxError(f"Unable to load pipe from {script_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        pipe_cls = None
        for value in vars(module).values():
            if isinstance(value, type) and issubclass(value, Pipe) and value is not Pipe:
                pipe_cls = value
                break

        if pipe_cls is None:
            raise PipeSyntaxError(f"{script_path} must define a subclass of Pipe")

        self._delegate = pipe_cls(arg_string)

    def set_sources(self, inputs: list[Source]):
        self._delegate.set_sources(inputs)

    @property
    def arity(self) -> int:
        return type(self._delegate).arity

    def next(self) -> Optional[dict]:
        return self._delegate.next()
