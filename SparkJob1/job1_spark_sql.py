from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, sum, collect_set
from pyspark.sql.functions import round as spark_round
import json, time, os

# =============================================================
# Inizializzazione Spark
# =============================================================

spark = SparkSession.builder \
    .appName("FlightData Job1 Spark SQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
os.makedirs("output_job1_sparksql", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

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

    # Caricamento e cast delle colonne
    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
    df = df.withColumn("arr_delay", col("arr_delay").cast("double")) \
           .withColumn("cancelled", col("cancelled").cast("integer")) \
           .withColumn("month",     col("month").cast("integer"))

    # Aggregazione per (carrier, aeroporto)
    grouped = df.groupBy("op_unique_carrier", "origin").agg(
        count("*").alias("num_flights"),
        spark_round(min(col("arr_delay")),             2).alias("arr_delay_min"),
        spark_round(max(col("arr_delay")),             2).alias("arr_delay_max"),
        spark_round(avg(col("arr_delay")),             2).alias("arr_delay_avg"),
        spark_round(sum(col("cancelled")) / count("*"), 4).alias("cancellation_rate"),
        collect_set("month").alias("months")
    ).collect()

    # Costruzione output JSON
    result = {}
    for row in grouped:
        carrier = row["op_unique_carrier"].strip()
        if carrier not in result:
            result[carrier] = []
        result[carrier].append({
            "airport": row["origin"],
            "data": {
                "num_flights":       row["num_flights"],
                "arr_delay_min":     row["arr_delay_min"],
                "arr_delay_max":     row["arr_delay_max"],
                "arr_delay_avg":     row["arr_delay_avg"],
                "cancellation_rate": row["cancellation_rate"],
                "months":            [MONTH_NAMES[m] for m in sorted(row["months"])]
            }
        })

    structured = [{"carrier": c, "airports": a} for c, a in result.items()]

    with open(f"output_job1_sparksql/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    print(f"{name} completato in {round(time.time() - start, 2)} secondi.")

spark.stop()