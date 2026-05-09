import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil

# Ispeziono il dataset
df = pd.read_csv("flight_data_2024.csv", nrows=5)
print(df.columns.tolist())
print(df.head())
print(df.dtypes)
print(df.shape)

# Effettuo la pulizia dei dati con PySpark
spark = SparkSession.builder \
    .appName("FlightDataCleaning") \
    .getOrCreate()

# Carico il dataset in un DataFrame di Spark
df_raw = spark.read.csv("flight_data_2024.csv", header=True, inferSchema=True)
print(f"Record iniziali: {df_raw.count()}")

# Seleziono solo le colonne rilevanti per i Job1 e Job2
cols_needed = [
    "year",
    "month",
    "day_of_month",
    "op_unique_carrier",     
    "origin",                 
    "origin_city_name",       
    "dest",               
    "dest_city_name",         
    "dep_delay",              
    "arr_delay",              
    "cancelled",              
    "cancellation_code",  
    "diverted",   
    "carrier_delay",          
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

df = df_raw.select(cols_needed)
print(f"Colonne selezionate: {len(df.columns)}")

# Rimuovo i record con valori nulli nelle colonne di interesse
mandatory_cols = [
    "year", "month",
    "op_unique_carrier",
    "origin", "dest",   
    "cancelled","diverted"
]
df = df.dropna(subset=mandatory_cols)
print(f"Dopo drop mandatory nulls: {df.count():,}")

# Esclusione voli divertiti 
df = df.filter(F.col("diverted") == 0)
print(f"Dopo esclusione diverted: {df.count():,}")
df = df.drop("diverted")

# Voli cancellati implicano NULL su dep_delay e arr_delay, mentre voli non cancellati devono avere valori validi su dep_delay e arr_delay.

df = df.filter(
    (F.col("cancelled") == 1) |
    (
        F.col("dep_delay").isNotNull() &
        F.col("arr_delay").isNotNull()
    )
)
print(f"Dopo gestione sui ritardi: {df.count():,}")

# Gestione cause ritardo
# Le colonne causa sono NULL quando:
# il volo è cancellato;
# volo operato senza ritardo;

delay_cause_cols = [
    "carrier_delay", "weather_delay", "nas_delay",
    "security_delay", "late_aircraft_delay"
]
for col_name in delay_cause_cols:
    df = df.withColumn(
        col_name,
        F.when(
            F.col("cancelled") == 0,
            F.coalesce(F.col(col_name), F.lit(0))  # operato: NULL → 0
        ).otherwise(F.lit(None))                    # cancellato: lascia NULL
    )
    
# Validazione Temporale: Verifichiamo che i mesi e i giorni siano in range validi 

df = df.filter(
    (F.col("month").between(1, 12)) & 
    (F.col("day_of_month").between(1, 31))
)
print(f"Dopo filtro temporale: {df.count():,}")


# Normalizzazione stringhe
df = df.withColumn("op_unique_carrier",
                   F.trim(F.upper(F.col("op_unique_carrier"))))
df = df.withColumn("origin",
                   F.trim(F.upper(F.col("origin"))))
df = df.withColumn("dest",
                   F.trim(F.upper(F.col("dest"))))

# Validazione e Mappatura Codici Cancellazione
valid_codes = ["A", "B", "C", "D"]

# Prima puliamo: se non è A,B,C,D diventa NULL
df = df.withColumn(
    "cancellation_code",
    F.when(F.col("cancellation_code").isin(valid_codes),
           F.col("cancellation_code"))
     .otherwise(F.lit(None))
)

# Trasformiamo i codici in nomi leggibili
df = df.withColumn(
    "cancellation_reason", # Creiamo una nuova colonna parlante
    F.when(F.col("cancellation_code") == "A", "Carrier")
     .when(F.col("cancellation_code") == "B", "Weather")
     .when(F.col("cancellation_code") == "C", "NAS")
     .when(F.col("cancellation_code") == "D", "Security")
     .otherwise("Not Cancelled")
)

print(f"Dopo la pulizia e la mappatura cancellazioni: {df.count():,}")

# Controllo coerenza cancelled / cancellation_code 
# Se cancelled=1 → cancellation_code dovrebbe essere valorizzato
# Se cancelled=0 → cancellation_code deve essere NULL


inconsistent_cancelled = df.filter(
    (F.col("cancelled") == 1) & F.col("cancellation_code").isNull()
).count()
inconsistent_not_cancelled = df.filter(
    (F.col("cancelled") == 0) & F.col("cancellation_code").isNotNull()
).count()

print(f"\n[QC] Voli cancellati senza cancellation_code: {inconsistent_cancelled:,}")
print(f"[QC] Voli non cancellati con cancellation_code: {inconsistent_not_cancelled:,}")

# Correggi il secondo caso (cancellation_code su voli operati → NULL)
df = df.withColumn(
    "cancellation_code",
    F.when(F.col("cancelled") == 0, F.lit(None))
     .otherwise(F.col("cancellation_code"))
)

# Report qualità finale 
count_raw   = df_raw.count()
count_clean = df.count()

print("\n" + "="*55)
print(f"  Record originali:          {count_raw:>10,}")
print(f"  Record dopo pulizia:       {count_clean:>10,}")
print(f"  Record eliminati:          {count_raw - count_clean:>10,}  "
      f"({(count_raw - count_clean)/count_raw*100:.1f}%)")
print("="*55)

print("\nDistribuzione cancelled:")
df.groupBy("cancelled").count().orderBy("cancelled").show()

print("\nCause cancellazione:")
df.filter(F.col("cancelled") == 1) \
  .groupBy("cancellation_code").count() \
  .orderBy("cancellation_code").show()

print("\nDistribuzione per mese:")
df.groupBy("month").count().orderBy("month").show()

print("\nTop 10 compagnie per numero voli:")
df.groupBy("op_unique_carrier").count() \
  .orderBy(F.desc("count")).show(10)

print("\nTop 10 aeroporti di partenza:")
df.groupBy("origin").count() \
  .orderBy(F.desc("count")).show(10)
  
# Salvataggio dataset pulito in CSV
output_path = "flight_data_2024_clean.csv"

df.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .option("sep", ",") \
    .csv(output_path + "_tmp")


tmp = output_path + "_tmp"
for f in os.listdir(tmp):
    if f.endswith(".csv"):
        shutil.move(os.path.join(tmp, f), output_path)
        break
shutil.rmtree(tmp)

print(f"\n[SUCCESS] Dataset pulito salvato in: {output_path}")
spark.stop()
