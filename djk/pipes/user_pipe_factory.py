import importlib.util
from djk.base import Pipe, Sink, ParsedToken, SyntaxError

class UserPipeFactory:
    @staticmethod
    def create(ptok: ParsedToken) -> Pipe | None:
        script_path = ptok.main
        try:
            spec = importlib.util.spec_from_file_location("user_pipe", script_path)
            if spec is None or spec.loader is None:
                raise SyntaxError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise SyntaxError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Pipe)
                and not issubclass(value, Sink)                
                and value is not Pipe
                and value.__module__ == module.__name__  # ← Key line
            ):
                return value(ptok)

        return None
