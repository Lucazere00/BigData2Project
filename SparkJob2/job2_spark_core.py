from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

conf = SparkConf().setAppName("FlightData Job2 Spark Core").setMaster("local[*]").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
os.makedirs("output_job2_sparkcore", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}
MONTH_ORDER = list(MONTH_NAMES.values())

def parse_line(line, idx_map):
    try:
        row = next(csv.reader(StringIO(line)))
        if row[idx_map["month"]] == "month": return None
        
        origin = row[idx_map["origin"]].strip()
        month = int(row[idx_map["month"]])
        cancelled = int(float(row[idx_map["cancelled"]]))
        
        dep_delay = float(row[idx_map["dep_delay"]]) if row[idx_map["dep_delay"]] else None
        arr_delay = float(row[idx_map["arr_delay"]]) if row[idx_map["arr_delay"]] else None
        
        # Mappatura Cause
        causes = []
        if cancelled == 1:
            reason = row[idx_map["cancellation_reason"]]
            if reason != "Not Cancelled": causes.append(f"Canc_{reason}")
        else:
            delay_cols = [("carrier_delay", "Delay_Carrier"), ("weather_delay", "Delay_Weather"), 
                          ("nas_delay", "Delay_NAS"), ("security_delay", "Delay_Security"), 
                          ("late_aircraft_delay", "Delay_LateAircraft")]
            for col_name, cause_name in delay_cols:
                val = row[idx_map[col_name]]
                if val and float(val) > 0: causes.append(cause_name)

        band = None
        if not cancelled and dep_delay is not None:
            if dep_delay < 15: band = "low"
            elif 15 <= dep_delay <= 60: band = "medium"
            elif dep_delay > 60: band = "high"

        return ((origin, month), (band, dep_delay, arr_delay, causes))
    except:
        return None

def create_combiner(val):
    band, dep_d, arr_d, causes = val
    stats = {"low": [0,0,0], "medium": [0,0,0], "high": [0,0,0]} # [count, sum_dep, sum_arr]
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

def merge_combiners(acc1, acc2):
    stats1, cause1 = acc1
    stats2, cause2 = acc2
    
    merged_stats = {}
    for b in ["low", "medium", "high"]:
        merged_stats[b] = [
            stats1[b][0] + stats2[b][0],
            stats1[b][1] + stats2[b][1],
            stats1[b][2] + stats2[b][2]
        ]
        
    merged_cause = cause1.copy()
    for c, count in cause2.items():
        merged_cause[c] = merged_cause.get(c, 0) + count
        
    return (merged_stats, merged_cause)

def build_final(acc):
    stats, cause_dict = acc
    
    def format_band(b_data):
        c, sum_dep, sum_arr = b_data
        return {
            "count": c,
            "avg_dep_delay": round(sum_dep/c, 2) if c > 0 else None,
            "avg_arr_delay": round(sum_arr/c, 2) if c > 0 else None
        }

    top_causes = [{"cause": k, "count": v} for k, v in sorted(cause_dict.items(), key=lambda item: item[1], reverse=True)][:3]
    
    return {
        "bands": {
            "low": format_band(stats["low"]),
            "medium": format_band(stats["medium"]),
            "high": format_band(stats["high"])
        },
        "top_3_causes": top_causes
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
    
    parsed = raw_data.filter(lambda l: l != header).map(lambda l: parse_line(l, IDX)).filter(lambda x: x is not None)
    aggregated = parsed.combineByKey(create_combiner, merge_value, merge_combiners).mapValues(build_final)

    structured = aggregated.map(lambda x: (x[0][0], {"month": MONTH_NAMES[x[0][1]], **x[1]})) \
                           .groupByKey().mapValues(lambda it: sorted(list(it), key=lambda x: MONTH_ORDER.index(x["month"]))) \
                           .map(lambda x: {"airport": x[0], "months": x[1]}).collect()

    with open(f"output_job2_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"✅ test_{name} completato in {round(time.time() - start, 2)} secondi.")

sc.stop()