# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/factory.py
from pjk.components import Pipe
from pjk.usage import Usage, ParsedToken
from pjk.common import ComponentFactory
from pjk.pipes.move_field import MoveField
from pjk.pipes.remove_field import RemoveField
from pjk.pipes.let_reduce import LetPipe
from pjk.pipes.let_reduce import ReducePipe
from pjk.pipes.head import HeadPipe
from pjk.pipes.tail import TailPipe
from pjk.pipes.sort import SortPipe
from pjk.pipes.where import WherePipe
from pjk.pipes.map import MapByPipe
from pjk.pipes.map import GroupByPipe
from pjk.pipes.join import JoinPipe
from pjk.pipes.filter import FilterPipe
from pjk.pipes.select import SelectFields
from pjk.pipes.denorm import DenormPipe
from pjk.integrations.postgres_pipe import PostgresPipe
from pjk.integrations.snowflake_pipe import SnowflakePipe
from pjk.integrations.opensearch_query_pipe import OpenSearchQueryPipe
from pjk.pipes.sample import SamplePipe
from pjk.pipes.user_pipe_factory import UserPipeFactory

COMPONENTS = {
        'head': HeadPipe,
        'tail': TailPipe,
        'join': JoinPipe,
        'filter': FilterPipe,
        'mapby': MapByPipe,            
        'groupby': GroupByPipe,
        'as': MoveField,
        'drop': RemoveField,        
        'let': LetPipe,
        'reduce': ReducePipe,        
        'sort': SortPipe,
        'where': WherePipe,
        'select': SelectFields,
        'sample': SamplePipe,
        'explode': DenormPipe,
        'postgres': PostgresPipe,
        'snowflake': SnowflakePipe,
        'os_query': OpenSearchQueryPipe
    }

class PipeFactory(ComponentFactory):
    def __init__(self):
        super().__init__(COMPONENTS)

    def get_comp_type_name(self):
        return 'pipe'

    def create(self, token: str) -> Pipe:

        ptok = ParsedToken(token)
        if ptok.pre_colon.endswith('.py'):
            pipe = UserPipeFactory.create(ptok)
            if pipe:
                return pipe # else keep looking

        pipe_cls = self.get_component_class(ptok.pre_colon)

        if not pipe_cls:
            return None
        
        usage = pipe_cls.usage()
        usage.bind(ptok)
        
        pipe = pipe_cls(ptok, usage)
        return pipe

