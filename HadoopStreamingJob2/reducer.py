#!/usr/bin/env python3
# reducer.py - Job 2
# Riceve righe ordinate per (origin|month), aggrega fasce di ritardo e cause

import sys
import json
from collections import defaultdict

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

DELAY_CAUSE_LABELS = [
    "Delay_Carrier",
    "Delay_Weather",
    "Delay_NAS",
    "Delay_Security",
    "Delay_LateAircraft"
]

# Struttura dati: airport -> month -> statistiche
# stats: {fascia: [count, sum_dep, sum_arr]}
results = defaultdict(lambda: defaultdict(lambda: {
    "stats":  {"low": [0, 0.0, 0.0], "medium": [0, 0.0, 0.0], "high": [0, 0.0, 0.0]},
    "causes": defaultdict(int)
}))

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue

        key, value = line.split("\t")
        origin, month = key.split("|")
        parts = value.split(",")

        dep_delay   = float(parts[0])
        arr_delay   = float(parts[1])
        cancelled   = int(float(parts[2]))
        canc_reason = parts[3]
        cause_vals  = [float(x) for x in parts[4:]]

        entry = results[origin][int(month)]

        # Fasce di ritardo (solo voli operati)
        if cancelled == 0:
            if dep_delay < 15:
                band = "low"
            elif dep_delay <= 60:
                band = "medium"
            else:
                band = "high"
            entry["stats"][band][0] += 1
            entry["stats"][band][1] += dep_delay
            entry["stats"][band][2] += arr_delay

        # Cause piu' frequenti
        if cancelled == 1:
            if canc_reason and canc_reason != "None":
                entry["causes"][f"Canc_{canc_reason}"] += 1
        else:
            for i, val in enumerate(cause_vals):
                if val > 0:
                    entry["causes"][DELAY_CAUSE_LABELS[i]] += 1

    except Exception:
        continue

# =============================================================
# Costruzione output JSON
# =============================================================

final_output = []
for airport in sorted(results.keys()):
    months_data = []
    for month in sorted(results[airport].keys()):
        data = results[airport][month]

        bands = {}
        for band in ["low", "medium", "high"]:
            count, sum_dep, sum_arr = data["stats"][band]
            bands[band] = {
                "count":         count,
                "avg_dep_delay": round(sum_dep / count, 2) if count > 0 else None,
                "avg_arr_delay": round(sum_arr / count, 2) if count > 0 else None
            }

        top_causes = [
            {"cause": k, "count": v}
            for k, v in sorted(data["causes"].items(), key=lambda x: x[1], reverse=True)
        ][:3]

        months_data.append({
            "month":        MONTH_NAMES[month],
            "bands":        bands,
            "top_3_causes": top_causes
        })

    final_output.append({"airport": airport, "months": months_data})

print(json.dumps(final_output, indent=4))