#!/usr/bin/env python3
# mapper.py - Job 2
# Emette coppie chiave-valore: (origin|month) -> (dep_delay, arr_delay, cancelled, canc_reason, cause1..5)
#
# Indici basati sul dataset pulito:
# month(0), op_unique_carrier(1), origin(2), dep_delay(3), arr_delay(4),
# cancelled(5), cancellation_code(6), carrier_delay(7), weather_delay(8),
# nas_delay(9), security_delay(10), late_aircraft_delay(11), cancellation_reason(12)

import sys
import csv

IDX_MONTH        = 0
IDX_ORIGIN       = 2
IDX_DEP_DELAY    = 3
IDX_ARR_DELAY    = 4
IDX_CANCELLED    = 5
IDX_CAUSE_START  = 7   # carrier_delay
IDX_CAUSE_END    = 11  # late_aircraft_delay
IDX_CANC_REASON  = 12

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        # Salta l'header se presente
        if row[0] == "month":
            continue

        origin    = row[IDX_ORIGIN].strip()
        month     = row[IDX_MONTH].strip()
        cancelled = row[IDX_CANCELLED].strip()

        if not origin or not month:
            continue

        dep_delay = row[IDX_DEP_DELAY].strip() or "0"
        arr_delay = row[IDX_ARR_DELAY].strip() or "0"

        # Cause di ritardo (5 valori: carrier, weather, nas, security, late_aircraft)
        cause_string = ",".join(
            row[i].strip() or "0" for i in range(IDX_CAUSE_START, IDX_CAUSE_END + 1)
        )

        # Causa di cancellazione (solo per voli cancellati)
        if int(float(cancelled)) == 1:
            canc_reason = row[IDX_CANC_REASON].strip()
        else:
            canc_reason = "None"

        print(f"{origin}|{month}\t{dep_delay},{arr_delay},{cancelled},{canc_reason},{cause_string}")

    except Exception:
        continue