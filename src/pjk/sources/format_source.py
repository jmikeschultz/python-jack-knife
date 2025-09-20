from pjk.base import Source, Usage

class SourceFormatUsage(Usage):
    def __init__(self, name: str, component_class: type, desc_override: str = None):
        desc = f'{name} source for s3 and local files/directories.' if desc_override == None else desc_override
        super().__init__(name, desc, component_class)

        self.def_syntax("") # no syntax for these
        self.def_example(expr_tokens=[f"myfile.{name}", "-"], expect=None)
        self.def_example(expr_tokens=["mydir", "-"], expect=None)
        self.def_example(expr_tokens=[f"s3://mybucket/myfile.{name}", "-"], expect=None)
        self.def_example(expr_tokens=["s3://mybucket/myfiles", "-"], expect=None)

class FormatSource(Source):
    extension: str = None
    desc_override:str = None

    @classmethod
    def usage(cls):
        return SourceFormatUsage(name=cls.extension,
                           component_class=cls,
                           desc_override=cls.desc_override)
