# djk/pipes/factory.py
from djk.base import Usage, Pipe, ParsedToken, ComponentFactory
from djk.pipes.move_field import MoveField
from djk.pipes.remove_field import RemoveField
from djk.pipes.add_field import AddField
from djk.pipes.head import HeadPipe
from djk.pipes.tail import TailPipe
from djk.pipes.sort import SortPipe
from djk.pipes.grep import GrepPipe
from djk.pipes.map import MapPipe
from djk.pipes.join import JoinPipe
from djk.pipes.filter import FilterPipe
from djk.pipes.select import SelectFields
from djk.pipes.subexp import SubExpression
from djk.pipes.subexp_over import SubExpressionOver
from djk.pipes.denorm import DenormPipe
from djk.pipes.group import GroupPipe
from djk.pipes.user_pipe_factory import UserPipeFactory

class PipeFactory(ComponentFactory):
    HEADER = 'PIPES'
    COMPONENTS = {
        'head': HeadPipe,
        'tail': TailPipe,
        'join': JoinPipe,
        'filter': FilterPipe,
        'map': MapPipe,        
        'mv': MoveField,
        'rm': RemoveField,        
        'let': AddField, # i.e. let a column 
        'sort': SortPipe,
        'grep': GrepPipe,
        'sel': SelectFields,
        'denorm': DenormPipe,
        'group': GroupPipe,
        '[': SubExpression,
        'over': SubExpressionOver
    }

    @classmethod
    def create(cls, token: str) -> Pipe:

        ptok = ParsedToken(token)
        if ptok.main.endswith('.py'):
            pipe = UserPipeFactory.create(ptok)
            if pipe:
                return pipe # else keep looking

        pipe_cls = cls.COMPONENTS.get(ptok.main)

        if not pipe_cls:
            return None
        
        usage = pipe_cls.usage()
        usage.bind(ptok)
        
        pipe = pipe_cls(ptok, usage)
        return pipe

