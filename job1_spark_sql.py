from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, sum, collect_set
from pyspark.sql.functions import round as spark_round
import json, time, os

# ── Inizializza Spark ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FlightData Job1 Spark SQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Caricamento dataset ───────────────────────────────────────
df = spark.read.option("header", True).option("inferSchema", True).csv("flight_data_2024_clean.csv")

# Cast colonne
df = df.withColumn("arr_delay",  col("arr_delay").cast("double")) \
       .withColumn("cancelled",  col("cancelled").cast("integer")) \
       .withColumn("month",      col("month").cast("integer"))

# ── Cartella output ───────────────────────────────────────────
os.makedirs("output_job1_sparksql", exist_ok=True)

# ── Test set con dimensioni crescenti ─────────────────────────
total = df.count()
test_sets = {
    "500k": df.sample(False, 500000 / total, seed=42),
    "1M":   df.sample(False, 1000000 / total, seed=42),
    "3M":   df.sample(False, 3000000 / total, seed=42),
    "full": df
}

for name, subset in test_sets.items():
    print(f"\n▶️  Inizio test_{name}...")
    start = time.time()

    # Aggrega per (carrier, aeroporto)
    grouped = subset.groupBy("op_unique_carrier", "origin").agg(
        count("*").alias("num_flights"),
        spark_round(min(col("arr_delay")), 2).alias("arr_delay_min"),
        spark_round(max(col("arr_delay")), 2).alias("arr_delay_max"),
        spark_round(avg(col("arr_delay")), 2).alias("arr_delay_avg"),
        spark_round(sum(col("cancelled")) / count("*"), 4).alias("cancellation_rate"),
        collect_set("month").alias("months")
    ).collect()

    # Ristruttura in carrier → lista aeroporti
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
                "months":            sorted(row["months"])
            }
        })

    # Costruisce lista finale
    structured = [{"carrier": carrier, "airports": airports}
                  for carrier, airports in result.items()]

    # Salva in JSON
    with open(f"output_job1_sparksql/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    duration = round(time.time() - start, 2)
    print(f"✅ test_{name} completato in {duration} secondi.")

spark.stop()