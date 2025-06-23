import os
from djk.base import Source, Pipe, PipeSyntaxError
from djk.sources.json_source import JsonSource
from djk.sources.s3_source import S3Source
from djk.sources.source_list import SourceListSource
from djk.sources.inline_source import InlineSource

class SourceFactory:

    @classmethod
    def _resolve(cls, name, parms) -> Source:
        if name.startswith("s3:"):
            return S3Source(name)
        if name.endswith(".json") or name.endswith(".json.gz") or 'format=json' in parms:
            return JsonSource(name)
            
        return None


    @classmethod
    def create(cls, token) -> Source:
        token = token.strip()

        if InlineSource.is_inline(token):
            return InlineSource(token)

        parts = token.split(',') # the separator for optional params
        name = parts[0]
        parms = parts[1] if len(parts) > 1 else ""

        if os.path.isdir(name):
            files = [os.path.join(name, f) for f in os.listdir(name) if os.path.isfile(os.path.join(name, f))]
            sources = []
            for file in files:
                source = cls._resolve(file, parms)
                if source:
                    sources.append(source)
                else:
                    print(f'fix me in source factory:{file}')
                    break
            
            if not sources:
                return None
            
            list_source = SourceListSource()
            list_source.set_sources(sources)
            return list_source
    
        source = cls._resolve(name, parms)
        if source:
            return source
     
        return None
