#!/usr/bin/env python
import sys
from djk.parser import DjkExpressionParser

def main():
    if len(sys.argv) < 2:
        print("Usage: pjk <token1> <token2> ...")
        return

    tokens = sys.argv[1:]
    parser = DjkExpressionParser(tokens)
    try:
        sink = parser.parse()
        sink.drain()
    except SyntaxError as e:
        print(e)
        sys.exit(1)

if __name__ == '__main__':
    main()
