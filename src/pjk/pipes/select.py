# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/select_pipe.py

from pjk.components import DeepCopyPipe
from pjk.dot_path import dot_path_select
from pjk.usage import Usage, ParsedToken, UsageError

class SelectFields(DeepCopyPipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='select',
            desc='Select specific fields from each record.',
            component_class=cls
        )
        usage.def_arg(name='fields', usage='Comma-separated field names or dotted paths to retain; preserves nested structure; literal keys take precedence')
        usage.def_example(expr_tokens=["{id:1, dir:'up', color:'blue'}", 'select:id,color'], expect="id: 1, color:'blue'")
        usage.def_example(expr_tokens=["{id:1, old_data:{bid:2, status:'PAUSED'}}", 'select:id,old_data.bid'], expect="{id:1, old_data:{bid:2}}")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        arg_string = usage.get_arg('fields')
        if not arg_string:
            raise UsageError("select:<f1,f2,...> requires at least one field")

        self.keep_fields = {f.strip() for f in arg_string.split(',') if f.strip()}
        if not self.keep_fields:
            raise UsageError("select must include at least one valid field name")

    def reset(self):
        pass  # stateless

    def __iter__(self):
        for record in self.left:
            yield dot_path_select(record, self.keep_fields)
