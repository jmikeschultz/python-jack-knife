# djk/pipes/subexp.py
from djk.pipes.common import add_operator
from typing import Optional, Any, List
from djk.base import Pipe, ParsedToken, Source, UsageError
from djk.pipes.user_pipe_factory import UserPipeFactory

# special upstream source put in subexp stack for flexibility
# when we don't know what that upstream source will be.
class UpstreamSource(Source):
    def __init__(self):
        self.data = []
        self.index = 0
        self.inner_source = None

    def set_source(self, source: Source):
        self.inner_source = source

    def set_list(self, items):
        self.index = 0
        self.data = items if items else []

    def add_item(self, rec):
        self.data.append(rec)

    def reset(self):
        self.data = []
    
    def next(self) -> Optional[dict]:
        if self.inner_source:
            return self.inner_source.next()

        if self.index >= len(self.data):
            return None
        item = self.data[self.index]
        self.index += 1
        return item
    
class SubExpression(Pipe):
    def __init__(self, ptok: ParsedToken):
        super().__init__(ptok)
        self.upstream_source = UpstreamSource()
        self.over_arg = None # set by method
        self.over_field = None
        self.subexp_stack: List[Any] = [self.upstream_source] # put list source on operand stack
        self.subexp_ops: List[Any] = [] # list of operators for parent to reset
        self.over_pipe = None
        
    def add_subop(self, op):
        self.subexp_ops.append(op)
        add_operator(op, self.subexp_stack)

    # over_arg can be either a field name (expecting a list)
    # or it can be a 1-to-many user_pipe
    # need to think on this, if it makes sense to allow pipes in general
    # the a one-to-many pipe seems very natural e.g. 1 query in -> many results out
    # and the subexpress can operate on the results stream into the 'child' field
    def set_over_arg(self, over_arg):
        self.over_arg = over_arg
        if over_arg.endswith('.py'):
            self.over_field = 'child' # where the new subrecs from the over_pipe will go, default as 'child' field
            self.over_pipe = UserPipeFactory.create(over_arg)
            self.upstream_source.set_source(self.over_pipe) # self.upstream_source already on stack
            self.subexp_ops.append(self.over_pipe) # so the pipe gets reset
        else:
            self.over_field = over_arg
        
    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        
        if self.over_pipe:
            one_rec_upstream = UpstreamSource()
            one_rec_upstream.add_item(record)
            self.over_pipe.set_sources([one_rec_upstream])

        else: # else its over:field, get field data        
            field_data = record.pop(self.over_field, None)
            if not field_data:
                return record
        
            if isinstance(field_data, list):
                self.upstream_source.set_list(field_data)
            else:
                self.upstream_source.set_list([field_data])

        out_recs = []
        pipe = self.subexp_stack[-1]

        # reset components that are being reused across records
        for op in self.subexp_ops:
            if isinstance(op, Pipe):
                op.reset()
        
        while True:
            rec = pipe.next()
            if rec == None:
                break
            out_recs.append(rec)
        record[self.over_field] = out_recs

        # result for parent
        for op in self.subexp_ops:
            get_subexp = getattr(op, "get_subexp_result", None)
            if get_subexp:
                name, value = get_subexp()
                if name:
                    record[name] = value

        return record

