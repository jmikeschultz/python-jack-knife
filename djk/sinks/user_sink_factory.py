import importlib.util
from djk.base import Source, Sink, SyntaxError, ParsedToken

class UserSinkFactory:
    @staticmethod
    def create_from_path(ptok: ParsedToken, input_source) -> Sink | None:
        script_path = ptok.main
        try:
            spec = importlib.util.spec_from_file_location("user_sink", script_path)
            if spec is None or spec.loader is None:
                raise SyntaxError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise SyntaxError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Sink)
                and value is not Sink
                and value.__module__ == module.__name__
            ):
                return value(ptok, input_source)

        return None

    @classmethod
    def create(cls, ptok: ParsedToken, source: Source) -> Sink:
        if ptok.main.endswith('.py'):
            sink = cls.create_from_path(ptok, source)
            if sink:
                return sink

        