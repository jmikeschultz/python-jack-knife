from pjk.base import Sink, ParsedToken, NoBindUsage
from pjk.sinks.s3_sink import S3Sink
from pjk.sinks.dir_sink import DirSink
from typing import IO
import re
import gzip

class SinkFormatUsage(NoBindUsage):
    def __init__(self, name: str, component_class: type, desc_override: str = None):
        desc = f'{name} source for s3 and local files/directories.\ns3 defaults to \'json\', others require format param' if desc_override == None else desc_override
        super().__init__(name, desc, component_class)

        self.def_syntax("") # don't use generated syntax for these, rely on examples
        self.def_param('format', 'file format', is_num=False, valid_values={'json', 'csv', 'tsv', 'json.gz', 'tsv.gz', 'csv.gz'}, default='json')
        self.def_example(expr_tokens=["{hello: 'world'}", f"myfile.{name}"], expect=None)
        self.def_example(expr_tokens=["{hello: 'world}", f"{name}:mydir"], expect=None)
        self.def_example(expr_tokens=["{hello: 'world'}", f"s3://mybucket/myfile.{name}"], expect=None)
        self.def_example(expr_tokens=["{hello: 'world'}", f"s3://mybucket/myfiles@format={name}"], expect=None)

class FormatSink(Sink):
    extension: str = None
    desc_override = None

    @classmethod
    def usage(cls):
        return SinkFormatUsage(name=cls.extension,
                               component_class=cls,
                               desc_override=cls.desc_override)

    def __init__(self, outfile: IO[str]):
        super().__init__(None, None)
        self.outfile = outfile

    def close(self):
        self.outfile.close()

    @classmethod
    def get_format_gz(cls, input:str):
        is_gz = False
        format = input
        if input.endswith('.gz'):
            is_gz = True
            format = input[:-3]
        return format, is_gz

    @classmethod
    def create(cls, ptok: ParsedToken, sinks):
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
        usage.bind_params(ptok) # only bind params

        is_single_file = ext is not None
        is_gz = False
        format = None

        if is_single_file:
            format, is_gz = cls.get_format_gz(ext)
        
        if pre_colon: # s3 and dir
            if pre_colon == 's3':
                if not format: # if not specified explicitly
                    format, is_gz = cls.get_format_gz(usage.get_param('format'))

                sink_class = sinks.get(format)
                if not sink_class or not issubclass(sink_class, FormatSink):
                    return None
                fileno = -1 if is_single_file else 0 # -1 tells s3 single file, no threading
                return S3Sink(sink_class, path_no_ext, is_gz, fileno)
        
            sink_class = sinks.get(pre_colon) # dir case
            if sink_class and issubclass(sink_class, FormatSink):
                if is_single_file:
                    raise('fix this exception, error using format:dir with format extension')
                return DirSink(sink_class, path_no_ext, is_gz, fileno=0)
        
        sink_class = sinks.get(format) # local single file
        if not sink_class or not issubclass(sink_class, FormatSink):
            return None
        
        if not is_single_file:
            raise('fix this exception')
        
        filename = f'{path_no_ext}.{ext}'
        # open the output file stream
        if is_gz:
            outfile = gzip.open(filename, "wt", encoding="utf-8", newline="")
        else:
            outfile = open(filename, "wt", encoding="utf-8", newline="")

        # instantiate the sink with the prepared stream
        sink = sink_class(outfile)
        return sink
        