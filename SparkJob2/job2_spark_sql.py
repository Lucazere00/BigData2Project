from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json, time, os

# =============================================================
# Inizializzazione Spark
# =============================================================

spark = SparkSession.builder \
    .appName("FlightData Job2 Spark SQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
os.makedirs("output_job2_sparksql", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}
MONTH_ORDER = list(MONTH_NAMES.values())

sizes = ["500k", "1M", "3M", "5M", "full"]

# =============================================================
# Esecuzione per ciascuna taglia
# =============================================================

for name in sizes:
    file_path = f"input/flight_{name}.csv"
    if not os.path.exists(file_path):
        print(f"Salto {name}: file {file_path} non trovato.")
        continue

    print(f"\nInizio {name}...")
    start = time.time()

    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

    # Creazione flag per le fasce di ritardo (solo voli non cancellati)
    df = df.withColumn("is_low",  F.when((F.col("cancelled") == 0) & (F.col("dep_delay") < 15),           1).otherwise(0))
    df = df.withColumn("is_med",  F.when((F.col("cancelled") == 0) & (F.col("dep_delay").between(15, 60)), 1).otherwise(0))
    df = df.withColumn("is_high", F.when((F.col("cancelled") == 0) & (F.col("dep_delay") > 60),            1).otherwise(0))

    # Aggregazione per aeroporto e mese
    grouped = df.groupBy("origin", "month").agg(
        # Fascia bassa
        F.sum("is_low").alias("low_count"),
        F.sum(F.when(F.col("is_low")  == 1, F.col("dep_delay")).otherwise(0)).alias("low_dep_sum"),
        F.sum(F.when(F.col("is_low")  == 1, F.col("arr_delay")).otherwise(0)).alias("low_arr_sum"),
        # Fascia media
        F.sum("is_med").alias("med_count"),
        F.sum(F.when(F.col("is_med")  == 1, F.col("dep_delay")).otherwise(0)).alias("med_dep_sum"),
        F.sum(F.when(F.col("is_med")  == 1, F.col("arr_delay")).otherwise(0)).alias("med_arr_sum"),
        # Fascia alta
        F.sum("is_high").alias("high_count"),
        F.sum(F.when(F.col("is_high") == 1, F.col("dep_delay")).otherwise(0)).alias("high_dep_sum"),
        F.sum(F.when(F.col("is_high") == 1, F.col("arr_delay")).otherwise(0)).alias("high_arr_sum"),
        # Cause di ritardo
        F.sum(F.when(F.col("carrier_delay")      > 0, 1).otherwise(0)).alias("cause_delay_carrier"),
        F.sum(F.when(F.col("weather_delay")       > 0, 1).otherwise(0)).alias("cause_delay_weather"),
        F.sum(F.when(F.col("nas_delay")           > 0, 1).otherwise(0)).alias("cause_delay_nas"),
        F.sum(F.when(F.col("security_delay")      > 0, 1).otherwise(0)).alias("cause_delay_security"),
        F.sum(F.when(F.col("late_aircraft_delay") > 0, 1).otherwise(0)).alias("cause_delay_late"),
        # Cause di cancellazione
        F.sum(F.when(F.col("cancellation_reason") == "Carrier",  1).otherwise(0)).alias("cause_canc_carrier"),
        F.sum(F.when(F.col("cancellation_reason") == "Weather",  1).otherwise(0)).alias("cause_canc_weather"),
        F.sum(F.when(F.col("cancellation_reason") == "NAS",      1).otherwise(0)).alias("cause_canc_nas"),
        F.sum(F.when(F.col("cancellation_reason") == "Security", 1).otherwise(0)).alias("cause_canc_security")
    ).collect()

    # Costruzione output JSON
    def safe_avg(total, count):
        return round(total / count, 2) if count and count > 0 else None

    results = {}
    for row in grouped:
        origin = row["origin"].strip()
        if origin not in results:
            results[origin] = []

        causes = {
            "Delay_Carrier":     row["cause_delay_carrier"],
            "Delay_Weather":     row["cause_delay_weather"],
            "Delay_NAS":         row["cause_delay_nas"],
            "Delay_Security":    row["cause_delay_security"],
            "Delay_LateAircraft":row["cause_delay_late"],
            "Canc_Carrier":      row["cause_canc_carrier"],
            "Canc_Weather":      row["cause_canc_weather"],
            "Canc_NAS":          row["cause_canc_nas"],
            "Canc_Security":     row["cause_canc_security"]
        }
        top_causes = [
            {"cause": k, "count": v}
            for k, v in sorted(causes.items(), key=lambda x: x[1], reverse=True)
            if v > 0
        ][:3]

        results[origin].append({
            "month": MONTH_NAMES[row["month"]],
            "bands": {
                "low":    {"count": row["low_count"],  "avg_dep_delay": safe_avg(row["low_dep_sum"],  row["low_count"]),  "avg_arr_delay": safe_avg(row["low_arr_sum"],  row["low_count"])},
                "medium": {"count": row["med_count"],  "avg_dep_delay": safe_avg(row["med_dep_sum"],  row["med_count"]),  "avg_arr_delay": safe_avg(row["med_arr_sum"],  row["med_count"])},
                "high":   {"count": row["high_count"], "avg_dep_delay": safe_avg(row["high_dep_sum"], row["high_count"]), "avg_arr_delay": safe_avg(row["high_arr_sum"], row["high_count"])}
            },
            "top_3_causes": top_causes
        })

    structured = [
        {"airport": k, "months": sorted(v, key=lambda x: MONTH_ORDER.index(x["month"]))}
        for k, v in results.items()
    ]

    with open(f"output_job2_sparksql/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"{name} completato in {round(time.time() - start, 2)} secondi.")

spark.stop()