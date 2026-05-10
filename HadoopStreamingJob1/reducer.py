#!/usr/bin/env python3
# reducer.py - Job 1
# Riceve righe ordinate per carrier, aggrega le statistiche per aeroporto
import sys
import json
from collections import defaultdict

MONTH_NAMES = {
    "1": "January", "2": "February", "3": "March", "4": "April",
    "5": "May", "6": "June", "7": "July", "8": "August",
    "9": "September", "10": "October", "11": "November", "12": "December"
}

# Struttura dati: carrier → airport → statistiche
data = defaultdict(lambda: defaultdict(lambda: {
    "num_flights": 0,
    "total_cancelled": 0,
    "delays": [],
    "months": set()
}))

for line in sys.stdin:
    try:
        key, value = line.strip().split("\t")
        carrier = key.strip()
        parts = value.strip().split(",")

        origin    = parts[0].strip()
        arr_delay = parts[1].strip()
        cancelled = int(float(parts[2].strip()))
        month     = int(parts[3].strip())

        d = data[carrier][origin]
        d["num_flights"]     += 1
        d["total_cancelled"] += cancelled
        d["months"].add(month)

        if arr_delay:
            d["delays"].append(float(arr_delay))

    except Exception:
        continue

# Costruisce output finale come JSON
result = []
for carrier, airports in data.items():
    airport_list = []
    for origin, stats in airports.items():
        delays = stats["delays"]
        count  = stats["num_flights"]
        airport_list.append({
            "airport": origin,
            "data": {
                "num_flights":       count,
                "arr_delay_min":     round(min(delays), 2) if delays else None,
                "arr_delay_max":     round(max(delays), 2) if delays else None,
                "arr_delay_avg":     round(sum(delays) / len(delays), 2) if delays else None,
                "cancellation_rate": round(stats["total_cancelled"] / count, 4) if count > 0 else 0,
                "months":            [MONTH_NAMES[str(m)] for m in sorted(stats["months"])]
            }
        })
    result.append({
        "carrier":  carrier,
        "airports": airport_list
    })

print(json.dumps(result, indent=4))