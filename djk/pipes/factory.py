# djk/pipes/factory.py
from djk.base import Source, Pipe, PipeSyntaxError
from djk.pipes.move_field import MoveField
from djk.pipes.remove_field import RemoveField
from djk.pipes.add_field import AddField
from djk.pipes.head import HeadPipe
from djk.pipes.tail import TailPipe
from djk.pipes.sort import SortPipe
from djk.pipes.grep import GrepPipe
from djk.pipes.map import MapPipe
from djk.pipes.join import JoinPipe
from djk.pipes.select import SelectFields
from djk.pipes.subexp import SubExpression
from djk.pipes.subexp_over import SubExpressionOver
from djk.pipes.pyfunc import PythonFunctionPipe
from djk.pipes.denorm import DenormPipe
from djk.pipes.group import GroupPipe

class PipeFactory:
    PIPE_OPERATORS = {
        'join': JoinPipe,                
        'map': MapPipe,        
        'mv': MoveField,
        'rm': RemoveField,        
        'add': AddField,
        'head': HeadPipe,
        'tail': TailPipe,
        'sort': SortPipe,
        'grep': GrepPipe,
        'select': SelectFields,
        'denorm': DenormPipe,
        'group': GroupPipe,
        '[': SubExpression,
        'over': SubExpressionOver
    }

    @classmethod
    def create(cls, token) -> Pipe:
        if token.endswith('.py'):
             return cls.create_pypipe(token)

        parts = token.split(':', 1)
        pipe_cls = cls.PIPE_OPERATORS.get(parts[0])

        if not pipe_cls:
            return None
        
        arg_string = "" if len(parts) == 1 else parts[1]
        pipe = pipe_cls(arg_string)
        return pipe

    @classmethod
    def create_pypipe(cls, token):
        return PythonFunctionPipe(token, "")

    @classmethod
    def instantiate_pipe(cls, token: str, input_source: Source) -> Pipe:
        parts = token.split(':', 1)
        op_name = parts[0]
        arg_string = parts[1] if len(parts) > 1 else ''
        pipe_cls = cls.PIPE_OPERATORS[op_name]
        try:
            return pipe_cls(input_source, arg_string)
        except PipeSyntaxError as e:
            raise SyntaxError(f"Invalid syntax for operator '{op_name}': {e}") from e
