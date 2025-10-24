# template for Source used by create sink

from pjk.components import Source
from pjk.usage import Usage, ParsedToken, CONFIG_FILE
import yaml
from pathlib import Path
import yaml
from pathlib import Path

class YamlRecords:
    def __init__(self, path):
        self.path = Path(path).expanduser()

    def __iter__(self):
        with self.path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("Top-level YAML must be a mapping of records")
        
        for name, body_dict in data.items():
            component, instance = name.split('-', 1)
            out = {
                'entry': name,
                'component': component,
                'instance': instance,
                'settings': body_dict
            }
            #out.update(body_dict)
            yield out

class ConfigsSource(Source):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='configs',
            desc=f'A source of pjk configuration in {CONFIG_FILE}',
            component_class=cls
    	)
        usage.def_example(expr_tokens=['configs', '-'], expect=None)
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        self.config_recs = YamlRecords(CONFIG_FILE)

    def __iter__(self):
        yield from self.config_recs

    def deep_copy(self):
        return None

    def close(self):
        pass
