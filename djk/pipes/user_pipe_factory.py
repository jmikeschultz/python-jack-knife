import importlib.util
from djk.base import Pipe, SyntaxError

class UserPipeFactory:
    @staticmethod
    def create(script_path: str, arg_string: str = "") -> Pipe:
        spec = importlib.util.spec_from_file_location("user_pipe", script_path)
        if spec is None or spec.loader is None:
            raise SyntaxError(f"Unable to load pipe from {script_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for value in vars(module).values():
            if isinstance(value, type) and issubclass(value, Pipe) and value is not Pipe:
                return value(arg_string)

        raise SyntaxError(f"{script_path} must define a subclass of Pipe")
