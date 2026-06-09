from pjk.parser import read_macros
from pjk.paths import macros_file_path
from pjk.usage import Usage, ParsedToken
from pjk.components import Source
from typing import Dict

class MacroSource(Source):
    @classmethod
    def usage(cls):
        u = Usage(
            name='macros',
            desc=f"Source to list the macro expressions stored in {macros_file_path()}.\n"
                  "A specific macro is referenced using 'm:<instance>, e.g. pjk m:hw -",
            component_class=cls
    	)
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        pass

    # only the instance=+ case comes here.  See parser
    def __iter__(self):
        macros = read_macros()
        for k, v in macros.items():
            yield {k: v}

    def deep_copy(self):
        return None

    def close(self):
        pass
