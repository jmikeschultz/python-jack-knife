import importlib.util
from djk.base import Pipe, Sink, ParsedToken, UsageError

class UserPipeFactory:
    @staticmethod
    def create(ptok: ParsedToken) -> Pipe | None:
        script_path = ptok.pre_colon
        try:
            spec = importlib.util.spec_from_file_location("user_pipe", script_path)
            if spec is None or spec.loader is None:
                raise UsageError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise UsageError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Pipe)
                and not issubclass(value, Sink)                
                and value is not Pipe
                and value.__module__ == module.__name__  # ← Key line
            ):
                usage = value.usage()
                usage.bind(ptok)
                return value(ptok, usage)
        return None
