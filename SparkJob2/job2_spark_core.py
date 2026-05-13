from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

# =============================================================
# Inizializzazione Spark
# =============================================================

conf = SparkConf() \
    .setAppName("FlightData Job2 Spark Core") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
os.makedirs("output_job2_sparkcore", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}
MONTH_ORDER = list(MONTH_NAMES.values())

# =============================================================
# Funzioni di parsing e aggregazione
# =============================================================

def parse_line(line, idx):
    try:
        row = next(csv.reader(StringIO(line)))
        if row[idx["month"]] == "month":
            return None

        origin    = row[idx["origin"]].strip()
        month     = int(row[idx["month"]])
        cancelled = int(float(row[idx["cancelled"]]))

        dep_delay = float(row[idx["dep_delay"]]) if row[idx["dep_delay"]] else None
        arr_delay = float(row[idx["arr_delay"]]) if row[idx["arr_delay"]] else None

        # Cause di cancellazione o ritardo
        causes = []
        if cancelled == 1:
            reason = row[idx["cancellation_reason"]]
            if reason != "Not Cancelled":
                causes.append(f"Canc_{reason}")
        else:
            delay_cols = [
                ("carrier_delay",      "Delay_Carrier"),
                ("weather_delay",      "Delay_Weather"),
                ("nas_delay",          "Delay_NAS"),
                ("security_delay",     "Delay_Security"),
                ("late_aircraft_delay","Delay_LateAircraft")
            ]
            for col_name, label in delay_cols:
                val = row[idx[col_name]]
                if val and float(val) > 0:
                    causes.append(label)

        # Fascia di ritardo
        band = None
        if not cancelled and dep_delay is not None:
            if dep_delay < 15:
                band = "low"
            elif dep_delay <= 60:
                band = "medium"
            else:
                band = "high"

        return ((origin, month), (band, dep_delay, arr_delay, causes))
    except Exception:
        return None

def create_combiner(val):
    band, dep_d, arr_d, causes = val
    # stats: {fascia: [count, sum_dep, sum_arr]}
    stats = {"low": [0, 0, 0], "medium": [0, 0, 0], "high": [0, 0, 0]}
    cause_dict = {}
    if band:
        stats[band][0] += 1
        stats[band][1] += dep_d
        stats[band][2] += arr_d if arr_d is not None else 0
    for c in causes:
        cause_dict[c] = cause_dict.get(c, 0) + 1
    return (stats, cause_dict)

def merge_value(acc, val):
    band, dep_d, arr_d, causes = val
    stats, cause_dict = acc
    if band:
        stats[band][0] += 1
        stats[band][1] += dep_d
        stats[band][2] += arr_d if arr_d is not None else 0
    for c in causes:
        cause_dict[c] = cause_dict.get(c, 0) + 1
    return (stats, cause_dict)

def merge_combiners(a, b):
    stats1, cause1 = a
    stats2, cause2 = b
    merged_stats = {
        band: [stats1[band][i] + stats2[band][i] for i in range(3)]
        for band in ["low", "medium", "high"]
    }
    merged_cause = cause1.copy()
    for c, count in cause2.items():
        merged_cause[c] = merged_cause.get(c, 0) + count
    return (merged_stats, merged_cause)

def build_final(acc):
    stats, cause_dict = acc

    def format_band(b):
        c, sum_dep, sum_arr = b
        return {
            "count":         c,
            "avg_dep_delay": round(sum_dep / c, 2) if c > 0 else None,
            "avg_arr_delay": round(sum_arr / c, 2) if c > 0 else None
        }

    top_causes = [
        {"cause": k, "count": v}
        for k, v in sorted(cause_dict.items(), key=lambda x: x[1], reverse=True)
    ][:3]

    return {
        "bands": {
            "low":    format_band(stats["low"]),
            "medium": format_band(stats["medium"]),
            "high":   format_band(stats["high"])
        },
        "top_3_causes": top_causes
    }

# =============================================================
# Esecuzione per ciascuna taglia
# =============================================================

sizes = ["500k", "1M", "3M", "5M", "full"]

for name in sizes:
    file_path = f"input/flight_{name}.csv"
    if not os.path.exists(file_path):
        continue

    print(f"\nInizio {name}...")
    start = time.time()

    raw    = sc.textFile(file_path)
    header = raw.first()
    IDX    = {col: i for i, col in enumerate(next(csv.reader(StringIO(header))))}

    parsed = raw.filter(lambda l: l != header) \
                .map(lambda l: parse_line(l, IDX)) \
                .filter(lambda x: x is not None)

    aggregated = parsed.combineByKey(create_combiner, merge_value, merge_combiners) \
                       .mapValues(build_final)

    structured = aggregated \
        .map(lambda x: (x[0][0], {"month": MONTH_NAMES[x[0][1]], **x[1]})) \
        .groupByKey() \
        .mapValues(lambda it: sorted(list(it), key=lambda x: MONTH_ORDER.index(x["month"]))) \
        .map(lambda x: {"airport": x[0], "months": x[1]}) \
        .collect()

    with open(f"output_job2_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"{name} completato in {round(time.time() - start, 2)} secondi.")

sc.stop()