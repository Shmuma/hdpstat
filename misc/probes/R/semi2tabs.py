#!/usr/bin/env python

"""
Script converts semicolon-separated probes data into tab-separated
"""

import sys

for l in sys.stdin:
    if l.startswith("attempt_"):
        continue
    v = l.strip().split(';')
    if len(v) != 3:
        v = [v[0], ";".join(v[1:-1]), v[-1]]
    print "\t".join(v)
