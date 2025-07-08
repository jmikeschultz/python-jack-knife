import importlib.util
from djk.base import Source, Pipe, Sink, ParsedToken, SyntaxError

class UserSourceFactory:
    @staticmethod
    def create(ptok: ParsedToken) -> Source | None:
        script_path = ptok.main
        try:
            spec = importlib.util.spec_from_file_location("user_source", script_path)
            if spec is None or spec.loader is None:
                raise SyntaxError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise SyntaxError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Source)
                and not issubclass(value, Pipe)
                and not issubclass(value, Sink)
                and value is not Source
                and value.__module__ == module.__name__  # 🧠 only user-defined classes
            ):
                return value(ptok.params)

        return None
