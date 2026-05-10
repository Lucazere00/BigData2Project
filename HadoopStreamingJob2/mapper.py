#!/usr/bin/env python3
import sys
import csv

# INDICI BASATI SULLA TUA LISTA
IDX_MONTH = 1
IDX_ORIGIN = 4
IDX_DEP_DELAY = 8
IDX_ARR_DELAY = 9
IDX_CANCELLED = 10
IDX_CAUSES_START = 12  # carrier_delay
IDX_CAUSES_END = 16    # late_aircraft_delay
IDX_CANC_REASON = 17

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        # Salta l'header se presente
        if not row or row[0] == "year":
            continue

        origin = row[IDX_ORIGIN].strip()
        month = row[IDX_MONTH].strip()
        cancelled = row[IDX_CANCELLED].strip()
        
        # Pulizia dati: se vuoti mettiamo "0"
        dep_delay = row[IDX_DEP_DELAY].strip() or "0"
        arr_delay = row[IDX_ARR_DELAY].strip() or "0"
        
        # Recupero le 5 cause di ritardo (indici 12, 13, 14, 15, 16)
        delays = [row[i].strip() or "0" for i in range(IDX_CAUSES_START, IDX_CAUSES_END + 1)]
        cause_string = ",".join(delays)
        
        # Recupero causa cancellazione (indice 17)
        canc_reason = row[IDX_CANC_REASON].strip() if int(float(cancelled)) == 1 else "None"

        if origin and month:
            # Chiave composta: Aeroporto|Mese
            print(f"{origin}|{month}\t{dep_delay},{arr_delay},{cancelled},{canc_reason},{cause_string}")
    except (IndexError, ValueError):
        continue