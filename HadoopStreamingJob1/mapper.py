#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        # Salta header
        if row[0] == "year":
            continue

        carrier   = row[3].strip()
        origin    = row[4].strip()
        month     = row[1].strip()
        arr_delay = row[9].strip()
        cancelled = row[10].strip()

        if not carrier or not origin:
            continue

        print(f"{carrier}\t{origin},{arr_delay},{cancelled},{month}")

    except Exception:
        continue