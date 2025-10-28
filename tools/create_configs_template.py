#!/usr/bin/env python3

import sys
from pjk.registry import ComponentRegistry

def get_entry_string(component_class):
    component_name = component_class.__name__
    return f'{component_name}-<instance>'

def print_entry(component_class, config_tuples, alias, out):
    component_name = component_class.__name__
    out.write(f'{get_entry_string(component_class)}:\n')
    if alias:
        out.write(f'   _alias: {get_entry_string(alias)}\n')

    else:
        for name, ptype, param_default in config_tuples:
            type_name = str(ptype).split("'")[1]
            out.write(f'   {name}: <{type_name}>\n')

    out.write('\n')
    
def main():
    out_path = sys.argv[2] if len(sys.argv) > 2 else "../src/pjk/resources/configs.tmpl"
    registry = ComponentRegistry()

    aliases = {}

    with open(out_path, "w", encoding="utf-8") as out:
        for factory in registry.get_factories():
            for _, comp_class in factory.get_component_name_class_tuples():
                usage = comp_class.usage()

                 # name, type, default
                config_tuples = usage.config_tuples
                if not config_tuples:
                    continue
                
                alias = aliases.get(id(config_tuples), None)
                print_entry(comp_class, config_tuples, alias, out)

                # this only works if two components point to the same config_tuples var
                aliases[id(config_tuples)] = comp_class


if __name__ == "__main__":
    main()
