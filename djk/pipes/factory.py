# djk/pipes/factory.py
from djk.base import Usage, Pipe, ParsedToken, ComponentFactory
from djk.pipes.move_field import MoveField
from djk.pipes.remove_field import RemoveField
from djk.pipes.let_reduce import LetPipe
from djk.pipes.let_reduce import ReducePipe
from djk.pipes.head import HeadPipe
from djk.pipes.tail import TailPipe
from djk.pipes.sort import SortPipe
from djk.pipes.where import WherePipe
from djk.pipes.map import MapPipe
from djk.pipes.join import JoinPipe
from djk.pipes.filter import FilterPipe
from djk.pipes.select import SelectFields
from djk.pipes.denorm import DenormPipe
from djk.pipes.user_pipe_factory import UserPipeFactory

class PipeFactory(ComponentFactory):
    HEADER = 'PIPES'
    COMPONENTS = {
        'head': HeadPipe,
        'tail': TailPipe,
        'join': JoinPipe,
        'filter': FilterPipe,
        'map': MapPipe,            
        'as': MoveField,
        'rm': RemoveField,        
        'let': LetPipe,
        'reduce': ReducePipe,        
        'sort': SortPipe,
        'where': WherePipe,
        'sel': SelectFields,
        'explode': DenormPipe,
    }

    @classmethod
    def create(cls, token: str) -> Pipe:

        ptok = ParsedToken(token)
        if ptok.all_but_params.endswith('.py'):
            pipe = UserPipeFactory.create(ptok)
            if pipe:
                return pipe # else keep looking

        pipe_cls = cls.COMPONENTS.get(ptok.pre_colon)

        if not pipe_cls:
            return None
        
        usage = pipe_cls.usage()
        usage.bind(ptok)
        
        pipe = pipe_cls(ptok, usage)
        return pipe

