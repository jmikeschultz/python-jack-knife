#!/usr/bin/env python3

import sys
from pjk.registry import ComponentRegistry

def print_entry(component_class, config_tuples, out):
    component_name = component_class.__name__
    out.write(f'{component_name}-<instance>:\n')
    for name, ptype, param_default in config_tuples:
        type_name = str(ptype).split("'")[1]
        out.write(f'   {name}: <{type_name}>\n')
    out.write('\n')
    
def main():
    out_path = sys.argv[2] if len(sys.argv) > 2 else "../src/pjk/resources/configs.tmpl"
    registry = ComponentRegistry()

    with open(out_path, "w", encoding="utf-8") as out:
        for factory in registry.get_factories():
            for _, comp_class in factory.get_component_name_class_tuples():
                usage = comp_class.usage()

                 # name, type, default
                config_tuples = usage.config_tuples
                if not config_tuples:
                    continue
                
                print_entry(comp_class, config_tuples, out)

if __name__ == "__main__":
    main()
