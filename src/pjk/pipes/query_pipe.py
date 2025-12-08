from pjk.components import Pipe
from pjk.usage import ParsedToken, Usage, CONFIG_FILE
from typing import Any, Dict, Iterable, Optional
from abc import abstractmethod
from pjk.progress import papi

class QueryPipe(Pipe):
    name: str = None
    desc: str = None
    arg0: tuple[Optional[str], Optional[str]] = (None, None)
    config_tuples = [] # name, type, default
    examples: list = []

    @classmethod
    def usage(cls):
        u = Usage(
            name=cls.name,
            desc=f"{cls.desc}\n"
            "The shape of output records is selected using the 'shape' parameter, default=xO.\n"
            "xO   = multiple (O)utput records.\n"
            "S_xO = a (S)ummary record followed by multiple (O)utput records.\n"
            "Sxo  = a single (S)ummary record containing multiple child (o)utput records."
            ,
            component_class=cls
        )
        u.def_arg(name=cls.arg0[0], usage=f"{cls.arg0[1]} {CONFIG_FILE} must contain entry '{cls.__name__}-<{cls.arg0[0]}>'\n  with necessary parameters.")
        u.def_param("count", usage="Number of search results, (databases may ignore)", is_num=True, default="10")
        u.def_param("shape", usage='the shape of ouput records', is_num=False,
                       valid_values={'xO', 'S_xO', 'Sxo'}, default='xO')

        for e in cls.examples:
            u.def_example(expr_tokens=e, expect=None)

        u.def_config_tuples(cls.config_tuples)
        return u


    def __init__(self, ptok: ParsedToken, usage: Usage, root = None):
        super().__init__(ptok, usage, root=root)
        self.output_shape = usage.get_param('shape')
        self.count = usage.get_param('count')
        self.query_field = 'query' # for all subclasses
        self.inrecs = papi.get_counter(self, var_label='recs_in') 
        self.outrecs = papi.get_percentage_counter(self, var_label='recs_out', denom_counter=self.inrecs)

    @abstractmethod
    def execute_query_returning_S_xO_iterable(self, record) -> Iterable[Dict[str, Any]]:
        pass

    def _make_q_object(self, in_rec: dict, result_header: dict):
        q = {}
        q['query_record'] = in_rec.copy()
        q['result_header'] = result_header
        return q

    def __iter__(self):
        for in_rec in self.left:
            self.inrecs.increment()
            iter = self.execute_query_returning_S_xO_iterable(in_rec)

            if self.output_shape == 'S_xO':
                q_done = False
                for out_rec in iter:
                    if not q_done:
                        q_done = True
                        self.outrecs.increment()
                        yield self._make_q_object(in_rec, out_rec)
                        continue

                    self.outrecs.increment()
                    yield out_rec

            elif self.output_shape == 'xO':
                q_done = False
                for out_rec in iter:
                    if not q_done:
                        q_done = True
                        continue
                    self.outrecs.increment()
                    yield out_rec

            elif self.output_shape == 'Sxo':
                q_done = False
                q_out = {}
                r_list = []

                for out_rec in iter:
                    if not q_done:
                        q_done = True
                        q_out = self._make_q_object(in_rec, out_rec)
                        continue
                    r_list.append(out_rec)
                q_out['child'] = r_list
                self.outrecs.increment()
                yield q_out


            
