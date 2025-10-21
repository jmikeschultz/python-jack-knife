from pjk.base import Pipe, ParsedToken, Usage
from typing import Any, Dict, Iterable, Optional
from abc import abstractmethod


class QueryPipe(Pipe):
    name: str = None
    desc: str = None
    arg0: tuple[Optional[str], Optional[str]] = (None, None)
    examples: list = []

    @classmethod
    def usage(cls):
        u = Usage(
            name=cls.name,
            desc=cls.desc,
            component_class=cls
        )
        u.def_arg(name=cls.arg0[0], usage=f"{cls.arg0[1]} ~/.pjk/component_configs.yaml must contain entry '{cls.__name__}-<{cls.arg0[0]}'>\n  with necessary parameters.")
        u.def_param("count", usage="Number of search results, (databases may ignore)", is_num=True, default="10")
        u.def_param("shape", usage='the shape of ouput records', is_num=False,
                       valid_values={'xR', 'Q_xR', 'Qxr'}, default='xR')

        for e in cls.examples:
            u.def_example(expr_tokens=e, expect=None)

        return u


    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.output_shape = usage.get_param('shape')
        self.count = usage.get_param('count')

    @abstractmethod
    def execute_query_returning_Q_xR_iterable(self, record) -> Iterable[Dict[str, Any]]:
        pass

    def _make_q_object(self, in_rec: dict, result_header: dict):
        q = {}
        q['query_record'] = in_rec.copy()
        q['result_header'] = result_header
        return q

    def __iter__(self):
        for in_rec in self.left:
            iter = self.execute_query_returning_Q_xR_iterable(in_rec)

            if self.output_shape == 'Q_xR':
                q_done = False
                for out_rec in iter:
                    if not q_done:
                        q_done = True
                        yield self._make_q_object(in_rec, out_rec)
                        continue
                    yield out_rec

            elif self.output_shape == 'xR':
                q_done = False
                for out_rec in iter:
                    if not q_done:
                        q_done = True
                        continue
                    yield out_rec

            elif self.output_shape == 'Qxr':
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
                yield q_out


            
