from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

# =============================================================
# Inizializzazione Spark
# =============================================================

conf = SparkConf() \
    .setAppName("FlightData Job1 Spark Core") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
os.makedirs("output_job1_sparkcore", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

# =============================================================
# Funzioni di parsing e aggregazione
# =============================================================

def parse_line(line, idx):
    try:
        row = next(csv.reader(StringIO(line)))
        if row[idx["month"]] == "month":
            return None

        carrier   = row[idx["op_unique_carrier"]].strip()
        origin    = row[idx["origin"]].strip()
        month     = int(row[idx["month"]])
        cancelled = int(float(row[idx["cancelled"]]))

        delay_raw = row[idx["arr_delay"]].strip()
        arr_delay = float(delay_raw) if delay_raw else None

        return ((carrier, origin), (arr_delay, cancelled, month))
    except Exception:
        return None

def create_combiner(value):
    arr_delay, cancelled, month = value
    delays = [arr_delay] if arr_delay is not None else []
    return (1, cancelled, delays, {month})

def merge_value(acc, value):
    count, total_cancelled, delays, months = acc
    arr_delay, cancelled, month = value
    if arr_delay is not None:
        delays.append(arr_delay)
    months.add(month)
    return (count + 1, total_cancelled + cancelled, delays, months)

def merge_combiners(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] | b[3])

def build_stats(acc):
    count, total_cancelled, delays, months = acc
    return {
        "num_flights":       count,
        "arr_delay_min":     round(min(delays), 2)              if delays else None,
        "arr_delay_max":     round(max(delays), 2)              if delays else None,
        "arr_delay_avg":     round(sum(delays) / len(delays), 2) if delays else None,
        "cancellation_rate": round(total_cancelled / count, 4)  if count > 0 else 0,
        "months":            [MONTH_NAMES[m] for m in sorted(months)]
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

    raw  = sc.textFile(file_path)
    header = raw.first()
    IDX  = {col: i for i, col in enumerate(next(csv.reader(StringIO(header))))}

    parsed = raw.filter(lambda l: l != header) \
                .map(lambda l: parse_line(l, IDX)) \
                .filter(lambda x: x is not None)

    aggregated = parsed.combineByKey(create_combiner, merge_value, merge_combiners) \
                       .mapValues(build_stats)

    structured = aggregated \
        .map(lambda x: (x[0][0], {"airport": x[0][1], "data": x[1]})) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: {"carrier": x[0], "airports": x[1]}) \
        .collect()

    with open(f"output_job1_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"{name} completato in {round(time.time() - start, 2)} secondi.")

sc.stop()