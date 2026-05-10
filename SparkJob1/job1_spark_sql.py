from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, sum, collect_set
from pyspark.sql.functions import round as spark_round
import json, time, os

# Inizializza Spark
spark = SparkSession.builder \
    .appName("FlightData Job1 Spark SQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
os.makedirs("output_job1_sparksql", exist_ok=True)

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

# Lista delle taglie da testare (nomi dei file fisici)
sizes = ["500k", "1M", "3M", "full"]

for name in sizes:
    file_path = f"input/flight_{name}.csv" # Assicurati che il percorso sia corretto
    if not os.path.exists(file_path):
        print(f"⚠️ Salto {name}: file {file_path} non trovato.")
        continue

    print(f"\n▶️  Inizio test_{name} (lettura da file)...")
    start = time.time()

    # Carica il file specifico
    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

    # Cast colonne
    df = df.withColumn("arr_delay",  col("arr_delay").cast("double")) \
           .withColumn("cancelled",  col("cancelled").cast("integer")) \
           .withColumn("month",      col("month").cast("integer"))

    # Aggrega per (carrier, aeroporto)
    grouped = df.groupBy("op_unique_carrier", "origin").agg(
        count("*").alias("num_flights"),
        spark_round(min(col("arr_delay")), 2).alias("arr_delay_min"),
        spark_round(max(col("arr_delay")), 2).alias("arr_delay_max"),
        spark_round(avg(col("arr_delay")), 2).alias("arr_delay_avg"),
        spark_round(sum(col("cancelled")) / count("*"), 4).alias("cancellation_rate"),
        collect_set("month").alias("months")
    ).collect()

    # Ristruttura l'output
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

    structured = [{"carrier": carrier, "airports": airports} for carrier, airports in result.items()]

    with open(f"output_job1_sparksql/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    duration = round(time.time() - start, 2)
    print(f"✅ test_{name} completato in {duration} secondi.")

spark.stop()