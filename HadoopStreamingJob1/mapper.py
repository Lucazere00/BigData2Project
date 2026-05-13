#!/usr/bin/env python3
# mapper.py - Job 1
# Emette coppie chiave-valore: carrier -> (origin, arr_delay, cancelled, month)
#
# Indici basati sul dataset pulito:
# month(0), op_unique_carrier(1), origin(2), dep_delay(3), arr_delay(4),
# cancelled(5), cancellation_code(6), carrier_delay(7), weather_delay(8),
# nas_delay(9), security_delay(10), late_aircraft_delay(11), cancellation_reason(12)

import sys
import csv

IDX_MONTH     = 0
IDX_CARRIER   = 1
IDX_ORIGIN    = 2
IDX_ARR_DELAY = 4
IDX_CANCELLED = 5

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        # Salta l'header se presente
        if row[0] == "month":
            continue

        carrier   = row[IDX_CARRIER].strip()
        origin    = row[IDX_ORIGIN].strip()
        month     = row[IDX_MONTH].strip()
        arr_delay = row[IDX_ARR_DELAY].strip()
        cancelled = row[IDX_CANCELLED].strip()

        if not carrier or not origin:
            continue

        print(f"{carrier}\t{origin},{arr_delay},{cancelled},{month}")

    except Exception:
        continue