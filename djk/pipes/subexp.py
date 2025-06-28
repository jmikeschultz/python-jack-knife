# djk/pipes/subexp.py
from djk.pipes.common import add_operator
from typing import Optional, Any, List
from djk.base import Pipe, Source, SyntaxError

class ListSource(Source):
    def __init__(self):
        self.data = None
        self.index = 0

    def set_list(self, items):
        self.index = 0
        self.data = items if items else []
    
    def next(self) -> Optional[dict]:
        if self.index >= len(self.data):
            return None
        item = self.data[self.index]
        self.index += 1
        return item
    
class SubExpression(Pipe):
    def __init__(self, arg_string: str = ""):
        super().__init__(arg_string)
        self.list_source = ListSource()
        self.over_field = None
        self.stack: List[Any] = [self.list_source] # put list source on operand stack
        self.op_list: List[Any] = [] # list of operators for parent 
        
    def add_subop(self, op):
        self.op_list.append(op)
        add_operator(op, self.stack)

    def set_over_field(self, field):
        self.over_field = field
        
    def next(self) -> Optional[dict]:
        record = self.inputs[0].next()
        if record is None:
            return None
        
        field_data = record.pop(self.over_field, None)
        if not field_data:
            return record
        
        if isinstance(field_data, list):
            self.list_source.set_list(field_data)
        else:
            self.list_source.set_list([field_data])

        out_recs = []
        pipe = self.stack[-1]

        # reset components that are being reused across records
        for op in self.op_list:
            if isinstance(op, Pipe):
                op.reset()
        
        while True:
            rec = pipe.next()
            if rec == None:
                break
            out_recs.append(rec)
        record[self.over_field] = out_recs

        # result for parent
        for op in self.op_list:
            get_subexp = getattr(op, "get_subexp_result", None)
            if get_subexp:
                name, value = get_subexp()
                if name:
                    record[name] = value

        return record

