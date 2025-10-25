from pjk.usage import Usage, ParsedToken
from pjk.components import Source
from pathlib import Path
from typing import Dict

FAVORITES_FILE = '~/.pjk/favorites.txt'

def read_favorites(file_name: str = FAVORITES_FILE) -> Dict[str, str]:
    out: Dict[str, str] = {}
    path = Path(file_name).expanduser()
    with path.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, val = line.split(":", 1)
            out[key.strip()] = val.strip()
    return out

class FavoriteSource(Source):
    @classmethod
    def usage(cls):
        u = Usage(
            name='fav',
            desc=f'favorite expressions stored in ~/.pjk/favorites.txt',
            component_class=cls
    	)
        u.def_arg(name='instance', usage="the instance of the favorite expression or '+' to list them all ")
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        self.once = False
        instance = usage.get_arg('instance')
        if not instance == '+':
            raise Exception('Programming eror: This cannot happen but did. Obviously.')

    # only the instance=+ case comes here.  See parser
    def __iter__(self):
        favs = read_favorites()
        for k, v in favs.items():
            yield {k: v}

    def deep_copy(self):
        return None

    def close(self):
        pass
