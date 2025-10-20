#!/usr/bin/env python3

import os
import re
import sys

CLASS_RE  = re.compile(r'^\s*class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[:(]')
CONFIG_RE = re.compile(r'Config\s*\(\s*self\s*,\s*(?P<q>["\'])(?P<key>[A-Za-z_][A-Za-z0-9_]*)\1')
LOOKUP_RE = re.compile(r'\blookup\s*\(\s*([\'"])(.+?)\1\s*\)')

class ConfigCollector:
    """
    Reads file lines.
    process(): if we see 'class X', set current class.
               if we later see 'Config(self,', mark config_seen for that class.
               while config_seen, every lookup("...") is collected for that class.
    print_template(out): emits one block per class that has config+lookups.
    """
    def __init__(self, path: str):
        self.path = path
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            self.lines = f.readlines()
        self.classes = {}  # name -> {"config_seen": bool, "lookups": list, "seen": set}

    def process(self):
        cur = None
        for line in self.lines:
            m = CLASS_RE.match(line)
            if m:
                cur = m.group(1)
                self.classes[cur] = {"config_seen": False, "lookups": [], "seen": set(), "lookup_key": None}
                continue

            if cur is None:
                continue

            m = CONFIG_RE.search(line)
            if m:
                self.classes[cur]["config_seen"] = True
                self.classes[cur]["lookup_key"] = m.group('key')
                continue

            if self.classes[cur]["config_seen"]:
                for lm in LOOKUP_RE.finditer(line):
                    key = lm.group(2)
                    if key not in self.classes[cur]["seen"]:
                        self.classes[cur]["seen"].add(key)
                        self.classes[cur]["lookups"].append(key)

    def print_template(self, out):
        for cls, data in self.classes.items():
            if not data["config_seen"] or not data["lookups"]:
                continue
            key = data['lookup_key']
            out.write(f"{cls}-<{key}>:\n")
            for k in data["lookups"]:
                out.write(f"  {k}: <{k}>\n")
            out.write("\n")


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "../src/pjk"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "../src/pjk/resources/component_configs.tmpl"

    collectors = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".py"):
                cc = ConfigCollector(os.path.join(dirpath, fn))
                cc.process()
                collectors.append(cc)

    with open(out_path, "w", encoding="utf-8") as out:
        for cc in collectors:
            cc.print_template(out)

if __name__ == "__main__":
    main()
