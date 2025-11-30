import re
import os
import shlex
from typing import List, Set
from pjk.common import pager_stdout, highlight

LOG_FILE = '.pjk-history.txt'

def printable_command(tokens: list) -> str:
    pattern = re.compile(r"[({]")

    return ' '.join(
        f'"{s}"' if pattern.search(s) else s
        for s in tokens
    )

def read_history(log_path: str) -> List[int]:
    """
    Reads the history file into an ordered dictionary (command -> ordinal)
    """
    # dict preserves insertion order in modern Python
    clist: List[str] = []
    cset:Set = set()

    try:
        with open(log_path, "r") as f:
            ordinal = 1
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if 'pjk ' in line: # legacy
                    line = line.split('pjk ', 1)[1]

                # Expected format: <command_string>
                line = line.strip()
                        
                # 2. Add to the map. Since dict keys must be unique,
                # this ensures the map only contains one entry per command, preserving the first seen's order.
                if line not in cset:
                    clist.append(line)
                    cset.add(line)
                            
    except FileNotFoundError:
        pass 
    except (PermissionError, OSError) as e:
        print(f"Warning: Could not read history file {log_path}: {e}")
        
    return clist, cset

def write_history(tokens: list):
    if os.environ.get("PJK_NO_HISTORY") == "1":
        return
    
    if len(tokens) < 2: 
        return 
    
    if tokens[0] == 'man':
        return
    
    new_command_string = printable_command(tokens)

    # 1. Read the existing history and find the highest number
    clist, cset = read_history(LOG_FILE)
    
    # 2. Check for duplicates (Fast O(1) lookup using the dict key)
    if new_command_string in cset:
        # Command is a duplicate, nothing to do.
        return
    
    # 3. Append the new command line to the file
    try:
        # Use 'a' to append the new line only
        with open(LOG_FILE, "a") as f:
            f.write(f"{new_command_string}\n")
            
    except (PermissionError, OSError) as e:
        print(f"Warning: Could not write to history file {LOG_FILE}: {e}")

def display_history():
    clist, cset = read_history(LOG_FILE)

    with pager_stdout():
        print(f"Local history in '{LOG_FILE}'")
        print("Use 'pjk +<#>' to execute command.")
        print()
        o = highlight('#', 'bold', '#')
        c = highlight('command', 'bold', 'command')
        print(f'{o}\t{c}')

        ordn = 1
        for command in reversed(clist):
            print(f'{ordn}\t{command}')
            ordn += 1

def get_history_tokens(ord_str: str):
    ord_in = int(ord_str)
    clist, cset = read_history(LOG_FILE)
    ordn = 0
    for command in reversed(clist):
        ordn += 1
        if ord_in == ordn:
            parts = shlex.split(command, comments=True, posix=True)
            return parts
    return None
