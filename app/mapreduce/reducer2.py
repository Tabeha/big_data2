#!/usr/bin/env python3
import sys

current_kind = None
current_key = None
current_sum = 0.0

n_docs = 0
dl_sum = 0.0


def flush(kind, key, value):
    global n_docs, dl_sum

    if kind is None:
        return

    if kind == "DF":
        print(f"TERM\t{key}\t{int(value)}")
    elif kind == "STAT":
        if key == "N":
            n_docs += int(value)
        elif key == "DL_SUM":
            dl_sum += float(value)


for raw in sys.stdin:
    raw = raw.rstrip("\n")
    if not raw:
        continue

    parts = raw.split("\t")
    if len(parts) != 3:
        continue

    kind, key, value = parts
    value = float(value)

    if current_kind == kind and current_key == key:
        current_sum += value
    else:
        flush(current_kind, current_key, current_sum)
        current_kind = kind
        current_key = key
        current_sum = value

flush(current_kind, current_key, current_sum)

print(f"STAT\tN\t{n_docs}")
avgdl = dl_sum / n_docs if n_docs > 0 else 0.0
print(f"STAT\tavgdl\t{avgdl}")