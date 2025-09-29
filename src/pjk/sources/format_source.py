from pjk.base import Source, NoBindUsage, ParsedToken
from pjk.sources.s3_source import S3Source
from pjk.sources.dir_source import DirSource
from pjk.sources.lazy_file_local import LazyFileLocal
import gzip
import re
import os

class SourceFormatUsage(NoBindUsage):
    def __init__(self, name: str, component_class: type, desc_override: str = None):
        desc = f'{name} source for s3 and local files/directories.' if desc_override == None else desc_override
        super().__init__(name, desc, component_class)

        self.def_syntax("") # no syntax for these
        # default = None because for source, format is an OVERRIDE
        self.def_param('format', 'file format', is_num=False, valid_values={'json', 'csv', 'tsv', 'json.gz', 'tsv.gz', 'csv.gz'}, default=None)
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
    
    @classmethod
    def get_format_gz(cls, input:str):
        is_gz = False
        format = input
        if input.endswith('.gz'):
            is_gz = True
            format = input[:-3]
        return format, is_gz

    #
    # THIS COPIED FROM format_sink, maybe can be unified?
    #    
    # A major difference between the format_sink and format_source for s3 and local dir 
    # is that with the format_sink you know the format BEFORE going to disk.  With
    # format_source you have to look what's inside to figure out what the format is.
    # For single files you can look at the extension.  (We could have required an extension on
    # directories/folders but that seemed not good.)
    #
    @classmethod
    def create(cls, ptok: ParsedToken, sources):
        """
        use cases covered:
        1) foo.<format>                 # local single file
        2) <format>:foo                 # local directory
        3) s3://bucket/prefix.<format>  # s3 single file
        4) s3://bucket/prefix           # s3 directory (@format=<format parameter with default = json)

        format = json, csv, tsv, and also json.gz etc.
        """

        pattern = re.compile(
            r'^(?:(?P<pre_colon>[^:]+):)?'            # optional precolon
            r'(?P<path>[^:]+?)'                      # main path
            r'(?:\.(?P<ext>\w+(?:\.gz)?))?$'         # optional extension, e.g. json, csv, json.gz
        )

        # we don't use framework token parsing (except for params) cuz too complicated
        input = ptok.all_but_params
        
        # Example usage
        match = pattern.match(input)
        if not match:
            return None
        
        gd = match.groupdict()
        pre_colon = gd.get('pre_colon', None)
        path_no_ext = gd.get('path', None)
        ext = gd.get('ext', None)

        usage = cls.usage()
        usage.bind_params(ptok) # just for params
        format_override = usage.get_param('format') # override what's specified in file extensions

        if not ext: # either local dir or s3
            if pre_colon and pre_colon == 's3': # could be single file, thus pass in ext
                return S3Source.create(sources, path_no_ext, ext, format_override=format_override)
            
            if os.path.isdir(path_no_ext):
                return DirSource.create(sources, path_no_ext, format_override=format_override)

            return None

        # else with ext, either local file or s3
        format, is_gz = cls.get_format_gz(ext)

        if pre_colon and pre_colon == 's3': # could be single file, thus pass in ext
            return S3Source.create(sources, path_no_ext, ext, format_override=format_override)
        
        if format_override:
            format, is_gz = cls.get_format_gz(format_override)

        source_class = sources.get(format) # local single file
        if not source_class or not issubclass(source_class, FormatSource):
            return None
        
        if not ext:
            raise('fix this exception')
        
        file = f'{path_no_ext}.{ext}'
        lazy_file = LazyFileLocal(file, is_gz)
        return source_class(lazy_file)
        
        