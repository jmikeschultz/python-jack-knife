from djk.base import Source, NoBindUsage

class FormatUsage(NoBindUsage):
    def __init__(self, name: str, component_class: type):
        desc = f'{name} source for s3 and local files/directories.'
        super().__init__(name, desc, component_class)

        self.def_syntax("") # no syntax for these
        self.def_example(expr_tokens=[f"myfile.{name}", "-"], expect=None)
        self.def_example(expr_tokens=["mydir", "-"], expect=None)
        self.def_example(expr_tokens=["s3://mybucket/path/to/files", "-"], expect=None)
