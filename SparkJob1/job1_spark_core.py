from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

conf = SparkConf().setAppName("FlightData Job1 Spark Core") \
                  .setMaster("local[*]") \
                  .set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

os.makedirs("output_job1_sparkcore", exist_ok=True)

def parse_line(line, idx_map):
    try:
        row = next(csv.reader(StringIO(line)))
        if row[idx_map["month"]] == "month": return None # Salta header extra
        
        carrier  = row[idx_map["op_unique_carrier"]].strip()
        origin   = row[idx_map["origin"]].strip()
        month    = int(row[idx_map["month"]])
        cancelled = int(float(row[idx_map["cancelled"]]))
        
        delay_raw = row[idx_map["arr_delay"]].strip()
        arr_delay = float(delay_raw) if delay_raw else None

        return ((carrier, origin), (arr_delay, cancelled, month))
    except:
        return None
# ── Funzioni aggregazione (combineByKey) ──────────────────────
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
    return (
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2],
        a[3] | b[3]
    )

def build_stats(acc):
    count, total_cancelled, delays, months = acc
    return {
        "num_flights": count,
        "arr_delay_min": round(min(delays), 2) if delays else None,
        "arr_delay_max": round(max(delays), 2) if delays else None,
        "arr_delay_avg": round(sum(delays) / len(delays), 2) if delays else None,
        "cancellation_rate": round(total_cancelled / count, 4) if count > 0 else 0,
        "months": sorted(months)
    }

sizes = ["500k", "1M", "3M", "full"]

for name in sizes:
    file_path = f"input/flight_{name}.csv"
    if not os.path.exists(file_path): continue

    print(f"\n▶️  Inizio test_{name}...")
    start = time.time()

    raw_data = sc.textFile(file_path)
    header = raw_data.first()
    fields = next(csv.reader(StringIO(header)))
    IDX = {field: i for i, field in enumerate(fields)}
    
    # Filtra header e parsa
    parsed = raw_data.filter(lambda l: l != header).map(lambda l: parse_line(l, IDX)).filter(lambda x: x is not None)

    aggregated = parsed.combineByKey(create_combiner, merge_value, merge_combiners).mapValues(build_stats)

    structured = aggregated.map(lambda x: (x[0][0], {"airport": x[0][1], "data": x[1]})) \
                           .groupByKey().mapValues(list) \
                           .map(lambda x: {"carrier": x[0], "airports": x[1]}).collect()

    with open(f"output_job1_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"✅ test_{name} completato in {round(time.time() - start, 2)} secondi.")

sc.stop()