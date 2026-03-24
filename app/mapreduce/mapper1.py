#!/usr/bin/env python3
import sys
import re
from collections import Counter

TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


for raw in sys.stdin:
    raw = raw.rstrip("\n")
    if not raw:
        continue

    parts = raw.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_text = parts
    tokens = tokenize(doc_text)
    doc_len = len(tokens)

    print(f"DOC\t{doc_id}\t{doc_title}\t{doc_len}")

    if doc_len == 0:
        continue

    tf_counter = Counter(tokens)
    for term, tf in tf_counter.items():
        print(f"POSTING\t{term}\t{doc_id}\t{tf}")