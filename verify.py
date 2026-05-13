#!/usr/bin/env python3
# verify_outputs.py
# Confronta gli output JSON di tutte le architetture per ciascun job e taglia.
# Riferimento: Spark SQL (considerata la più affidabile).
# Per ogni discrepanza trovata, stampa un report dettagliato.

import json
import os
from collections import defaultdict

SIZES = ["500k", "1M", "3M", "5M", "full"]

# =============================================================
# Percorsi degli output
# =============================================================

def get_path(job, arch, size):
    if arch == "mapreduce":
        return f"HadoopStreamingJob{job}/output_job{job}_mapreduce/{size}/risultato_{size}.json"
    elif arch == "sparksql":
        return f"output_job{job}_sparksql/{size}.json"
    elif arch == "sparkcore":
        return f"output_job{job}_sparkcore/{size}.json"

# =============================================================
# Normalizzazione output Job1
# Struttura attesa: lista di {carrier, airports: [{airport, data}]}
# Convertiamo in dizionario: {carrier -> {airport -> data}}
# =============================================================

def normalize_job1(data):
    result = {}
    for entry in data:
        carrier = entry["carrier"].strip()
        result[carrier] = {}
        for a in entry["airports"]:
            airport = a["airport"].strip()
            d = a["data"]
            result[carrier][airport] = {
                "num_flights":       d.get("num_flights"),
                "arr_delay_min":     d.get("arr_delay_min"),
                "arr_delay_max":     d.get("arr_delay_max"),
                "arr_delay_avg":     d.get("arr_delay_avg"),
                "cancellation_rate": d.get("cancellation_rate"),
                "months":            sorted(d.get("months", []))
            }
    return result

# =============================================================
# Normalizzazione output Job2
# Struttura attesa: lista di {airport, months: [{month, bands, top_3_causes}]}
# Convertiamo in dizionario: {airport -> {month -> data}}
# =============================================================

def normalize_job2(data):
    result = {}
    for entry in data:
        airport = entry["airport"].strip()
        result[airport] = {}
        for m in entry["months"]:
            month = m["month"]
            result[airport][month] = {
                "bands":        m.get("bands"),
                "top_3_causes": sorted(m.get("top_3_causes", []), key=lambda x: x["cause"])
            }
    return result

# =============================================================
# Confronto tra due dizionari normalizzati
# =============================================================

def compare_job1(ref, other, ref_name, other_name, size):
    errors = []

    # Carrier mancanti o in più
    for carrier in ref:
        if carrier not in other:
            errors.append(f"  Carrier '{carrier}' presente in {ref_name} ma assente in {other_name}")
    for carrier in other:
        if carrier not in ref:
            errors.append(f"  Carrier '{carrier}' presente in {other_name} ma assente in {ref_name}")

    # Confronto per ogni carrier/aeroporto
    for carrier in ref:
        if carrier not in other:
            continue
        for airport in ref[carrier]:
            if airport not in other[carrier]:
                errors.append(f"  [{carrier}] Aeroporto '{airport}' assente in {other_name}")
                continue
            d_ref = ref[carrier][airport]
            d_oth = other[carrier][airport]
            for field in ["num_flights", "arr_delay_min", "arr_delay_max", "arr_delay_avg", "cancellation_rate"]:
                v_ref = d_ref.get(field)
                v_oth = d_oth.get(field)
                if v_ref != v_oth:
                    errors.append(f"  [{carrier}][{airport}] {field}: {ref_name}={v_ref} vs {other_name}={v_oth}")
            if d_ref["months"] != d_oth["months"]:
                errors.append(f"  [{carrier}][{airport}] months: {ref_name}={d_ref['months']} vs {other_name}={d_oth['months']}")

    return errors

def compare_job2(ref, other, ref_name, other_name, size):
    errors = []

    for airport in ref:
        if airport not in other:
            errors.append(f"  Aeroporto '{airport}' presente in {ref_name} ma assente in {other_name}")
            continue
        for month in ref[airport]:
            if month not in other[airport]:
                errors.append(f"  [{airport}] Mese '{month}' assente in {other_name}")
                continue
            d_ref = ref[airport][month]
            d_oth = other[airport][month]

            # Confronto fasce
            for band in ["low", "medium", "high"]:
                for field in ["count", "avg_dep_delay", "avg_arr_delay"]:
                    v_ref = d_ref["bands"][band].get(field)
                    v_oth = d_oth["bands"][band].get(field)
                    if v_ref != v_oth:
                        errors.append(f"  [{airport}][{month}][{band}] {field}: {ref_name}={v_ref} vs {other_name}={v_oth}")

            # Confronto top 3 cause
            causes_ref = d_ref["top_3_causes"]
            causes_oth = d_oth["top_3_causes"]
            if causes_ref != causes_oth:
                errors.append(f"  [{airport}][{month}] top_3_causes: {ref_name}={causes_ref} vs {other_name}={causes_oth}")

    for airport in other:
        if airport not in ref:
            errors.append(f"  Aeroporto '{airport}' presente in {other_name} ma assente in {ref_name}")

    return errors

# =============================================================
# Esecuzione verifica
# =============================================================

def load(path, job):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    return normalize_job1(data) if job == 1 else normalize_job2(data)

total_errors = 0

for job in [1, 2]:
    normalize = normalize_job1 if job == 1 else normalize_job2
    compare   = compare_job1   if job == 1 else compare_job2

    print(f"\n{'='*60}")
    print(f"  JOB {job}")
    print(f"{'='*60}")

    for size in SIZES:
        print(f"\n  Taglia: {size}")

        ref_path = get_path(job, "sparksql", size)
        ref = load(ref_path, job)
        if ref is None:
            print(f"    [SKIP] File di riferimento non trovato: {ref_path}")
            continue

        for arch in ["sparkcore", "mapreduce"]:
            path = get_path(job, arch, size)
            other = load(path, job)
            if other is None:
                print(f"    [SKIP] {arch}: file non trovato ({path})")
                continue

            errors = compare(ref, other, "sparksql", arch, size)
            if not errors:
                print(f"    [OK] sparksql == {arch}")
            else:
                total_errors += len(errors)
                print(f"    [FAIL] sparksql vs {arch}: {len(errors)} discrepanze")
                for e in errors[:10]:  # Mostra al massimo 10 errori per coppia
                    print(e)
                if len(errors) > 10:
                    print(f"    ... e altri {len(errors) - 10} errori.")

print(f"\n{'='*60}")
if total_errors == 0:
    print("  Tutti gli output corrispondono.")
else:
    print(f"  Totale discrepanze trovate: {total_errors}")
print(f"{'='*60}\n")