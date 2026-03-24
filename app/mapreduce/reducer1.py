#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    print(line)