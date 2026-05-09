from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

# ── Configurazione Spark ──────────────────────────────────────
conf = SparkConf().setAppName("FlightData Job1 Spark Core") \
                  .setMaster("local[*]") \
                  .set("spark.driver.memory", "4g") \
                  .set("spark.driver.maxResultSize", "2g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# ── Caricamento dataset ───────────────────────────────────────
lines = sc.textFile("flight_data_2024_clean.csv")
header = lines.first()
data = lines.filter(lambda row: row != header)
total_records = data.count()
print(f"Record totali: {total_records:,}")

# ── Indici colonne ────────────────────────────────────────────
# year,month,day_of_month,op_unique_carrier,origin,origin_city_name,
# dest,dest_city_name,dep_delay,arr_delay,cancelled,cancellation_code,
# carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay
fields = next(csv.reader(StringIO(header)))
IDX = {field: i for i, field in enumerate(fields)}

# ── Parsing ───────────────────────────────────────────────────
def parse_line(line):
    try:
        row = next(csv.reader(StringIO(line)))
        carrier  = row[IDX["op_unique_carrier"]].strip()
        origin   = row[IDX["origin"]].strip()
        month    = int(row[IDX["month"]])
        cancelled = int(float(row[IDX["cancelled"]]))

        # arr_delay può essere NULL per voli cancellati
        arr_delay_raw = row[IDX["arr_delay"]].strip()
        arr_delay = float(arr_delay_raw) if arr_delay_raw else None

        if not carrier or not origin:
            return None

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

# ── Cartella output ───────────────────────────────────────────
os.makedirs("output_job1_sparkcore", exist_ok=True)

# ── Test set con dimensioni crescenti ─────────────────────────
limits = {"500k": 500000, "1M": 1000000, "3M": 3000000, "full": None}

for name, limit in limits.items():
    print(f"\n▶️  Inizio test_{name}...")
    start = time.time()

    subset = data if limit is None else data.sample(False, limit / total_records, seed=42)

    parsed = subset.map(parse_line).filter(lambda x: x is not None)

    # Aggrega per (carrier, origin)
    aggregated = parsed.combineByKey(
        create_combiner,
        merge_value,
        merge_combiners
    ).mapValues(build_stats)

    # Ristruttura: carrier → lista aeroporti
    carrier_airports = aggregated.map(
        lambda x: (x[0][0], {"airport": x[0][1], "data": x[1]})
    ).groupByKey().mapValues(list).cache()

    # Raccoglie tutti i carrier
    structured = carrier_airports.map(
        lambda x: {"carrier": x[0], "airports": x[1]}
    ).collect()

    # Salva in JSON
    with open(f"output_job1_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    carrier_airports.unpersist()
    duration = round(time.time() - start, 2)
    print(f"✅ test_{name} completato in {duration} secondi.")

sc.stop()