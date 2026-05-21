from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json, time

BUCKET = "s3://bigdata-project-wn"

spark = SparkSession.builder \
    .appName("FlightData Job2 Spark SQL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

MONTH_NAMES = {
    1: "January", 2: "February",  3: "March",    4: "April",
    5: "May",     6: "June",      7: "July",      8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}
MONTH_ORDER = list(MONTH_NAMES.values())

sizes = ["500k", "1M", "3M", "5M", "full"]
timing_results = []

for name in sizes:
    file_path   = f"{BUCKET}/input/flight_{name}.csv"
    output_path = f"{BUCKET}/output_job2_sparksql/{name}"

    print(f"\nInizio {name}...")
    start = time.time()

    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

    df = df.withColumn("is_low",  F.when((F.col("cancelled") == 0) & (F.col("dep_delay") < 15),           1).otherwise(0))
    df = df.withColumn("is_med",  F.when((F.col("cancelled") == 0) & (F.col("dep_delay").between(15, 60)), 1).otherwise(0))
    df = df.withColumn("is_high", F.when((F.col("cancelled") == 0) & (F.col("dep_delay") > 60),            1).otherwise(0))

    grouped = df.groupBy("origin", "month").agg(
        F.sum("is_low").alias("low_count"),
        F.sum(F.when(F.col("is_low")  == 1, F.col("dep_delay")).otherwise(0)).alias("low_dep_sum"),
        F.sum(F.when(F.col("is_low")  == 1, F.col("arr_delay")).otherwise(0)).alias("low_arr_sum"),
        F.sum("is_med").alias("med_count"),
        F.sum(F.when(F.col("is_med")  == 1, F.col("dep_delay")).otherwise(0)).alias("med_dep_sum"),
        F.sum(F.when(F.col("is_med")  == 1, F.col("arr_delay")).otherwise(0)).alias("med_arr_sum"),
        F.sum("is_high").alias("high_count"),
        F.sum(F.when(F.col("is_high") == 1, F.col("dep_delay")).otherwise(0)).alias("high_dep_sum"),
        F.sum(F.when(F.col("is_high") == 1, F.col("arr_delay")).otherwise(0)).alias("high_arr_sum"),
        F.sum(F.when(F.col("carrier_delay")      > 0, 1).otherwise(0)).alias("cause_delay_carrier"),
        F.sum(F.when(F.col("weather_delay")       > 0, 1).otherwise(0)).alias("cause_delay_weather"),
        F.sum(F.when(F.col("nas_delay")           > 0, 1).otherwise(0)).alias("cause_delay_nas"),
        F.sum(F.when(F.col("security_delay")      > 0, 1).otherwise(0)).alias("cause_delay_security"),
        F.sum(F.when(F.col("late_aircraft_delay") > 0, 1).otherwise(0)).alias("cause_delay_late"),
        F.sum(F.when(F.col("cancellation_reason") == "Carrier",  1).otherwise(0)).alias("cause_canc_carrier"),
        F.sum(F.when(F.col("cancellation_reason") == "Weather",  1).otherwise(0)).alias("cause_canc_weather"),
        F.sum(F.when(F.col("cancellation_reason") == "NAS",      1).otherwise(0)).alias("cause_canc_nas"),
        F.sum(F.when(F.col("cancellation_reason") == "Security", 1).otherwise(0)).alias("cause_canc_security")
    ).collect()

    def safe_avg(total, count):
        return round(total / count, 2) if count and count > 0 else None

    results = {}
    for row in grouped:
        origin = row["origin"].strip()
        if origin not in results:
            results[origin] = []
        causes = {
            "Delay_Carrier":      row["cause_delay_carrier"],
            "Delay_Weather":      row["cause_delay_weather"],
            "Delay_NAS":          row["cause_delay_nas"],
            "Delay_Security":     row["cause_delay_security"],
            "Delay_LateAircraft": row["cause_delay_late"],
            "Canc_Carrier":       row["cause_canc_carrier"],
            "Canc_Weather":       row["cause_canc_weather"],
            "Canc_NAS":           row["cause_canc_nas"],
            "Canc_Security":      row["cause_canc_security"]
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

    json_rdd = spark.sparkContext.parallelize([json.dumps(structured, indent=4)])
    json_rdd.coalesce(1).saveAsTextFile(output_path)

    duration = round(time.time() - start, 2)
    timing_results.append(f"{name}: {duration} secondi")
    print(f"{name} completato in {duration} secondi.")

timing_rdd = spark.sparkContext.parallelize(timing_results)
timing_rdd.coalesce(1).saveAsTextFile(f"{BUCKET}/timings/job2_sparksql")

spark.stop()