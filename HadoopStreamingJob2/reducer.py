#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

# Struttura: airport -> month -> dati
results = defaultdict(lambda: defaultdict(lambda: {
    "stats": {"low": [0, 0.0, 0.0], "medium": [0, 0.0, 0.0], "high": [0, 0.0, 0.0]},
    "causes": defaultdict(int)
}))

for line in sys.stdin:
    try:
        line = line.strip()
        if not line: continue
        
        key, val = line.split("\t")
        origin, month = key.split("|")
        parts = val.split(",")
        
        dep_delay = float(parts[0])
        arr_delay = float(parts[1])
        cancelled = int(float(parts[2]))
        canc_reason = parts[3]
        # Le 5 cause di ritardo sono in parts[4], parts[5], parts[6], parts[7], parts[8]
        delay_causes_vals = [float(x) for x in parts[4:]]

        m_stats = results[origin][int(month)]

        # --- A. FASCE DI RITARDO (Solo per voli NON cancellati) ---
        if cancelled == 0:
            if dep_delay < 15:
                band = "low"
            elif 15 <= dep_delay <= 60:
                band = "medium"
            else:
                band = "high"
            
            m_stats["stats"][band][0] += 1          # Conteggio voli
            m_stats["stats"][band][1] += dep_delay  # Somma ritardi partenza
            m_stats["stats"][band][2] += arr_delay  # Somma ritardi arrivo
        
        # --- B. CAUSE PIÙ FREQUENTI ---
        if cancelled == 1:
            if canc_reason and canc_reason != "None":
                m_stats["causes"][f"Canc_{canc_reason}"] += 1
        else:
            labels = ["Delay_Carrier", "Delay_Weather", "Delay_NAS", "Delay_Security", "Delay_Late"]
            for i, val_cause in enumerate(delay_causes_vals):
                if val_cause > 0:
                    m_stats["causes"][labels[i]] += 1
    except Exception:
        continue

# --- C. GENERAZIONE OUTPUT FINALE (JSON) ---
final_output = []
for airp in sorted(results.keys()):
    months_data = []
    for m in sorted(results[airp].keys()):
        data = results[airp][m]
        
        band_results = {}
        for b in ["low", "medium", "high"]:
            count = data["stats"][b][0]
            band_results[b] = {
                "count": count,
                "avg_dep_delay": round(data["stats"][b][1]/count, 2) if count > 0 else None,
                "avg_arr_delay": round(data["stats"][b][2]/count, 2) if count > 0 else None
            }
        
        # Top 3 cause
        top3 = [{"cause": k, "count": v} for k, v in sorted(data["causes"].items(), key=lambda x: -x[1])][:3]
        
        months_data.append({
            "month": m,
            "bands": band_results,
            "top_3_causes": top3
        })
        
    final_output.append({"airport": airp, "months": months_data})

print(json.dumps(final_output, indent=4))