#!/usr/bin/env python3
import sys

for raw in sys.stdin:
    raw = raw.rstrip("\n")
    if not raw:
        continue

    parts = raw.split("\t")
    tag = parts[0]

    if tag == "POSTING" and len(parts) == 4:
        _, term, doc_id, tf = parts
        print(f"DF\t{term}\t1")
    elif tag == "DOC" and len(parts) == 4:
        _, doc_id, doc_title, doc_len = parts
        print("STAT\tN\t1")
        print(f"STAT\tDL_SUM\t{doc_len}")