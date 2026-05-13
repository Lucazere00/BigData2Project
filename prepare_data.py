import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil

# =============================================================
# 0. Ispezione rapida del dataset grezzo con pandas
# =============================================================

df_pd = pd.read_csv("flight_data_2024.csv", nrows=5)
print(df_pd.columns.tolist())
print(df_pd.head())

# =============================================================
# 1. Caricamento con PySpark
# =============================================================

spark = SparkSession.builder \
    .appName("FlightDataPreparation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_raw = spark.read.csv("flight_data_2024.csv", header=True, inferSchema=True)
print(f"\nRecord iniziali: {df_raw.count():,}")

# =============================================================
# 2. Selezione delle colonne rilevanti per Job1 e Job2
# =============================================================

cols_needed = [
    "month",
    "op_unique_carrier",
    "origin",
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

# =============================================================
# 3. Rimozione record con valori nulli nelle colonne obbligatorie
# =============================================================

mandatory_cols = ["month", "op_unique_carrier", "origin", "cancelled", "diverted"]
df = df.dropna(subset=mandatory_cols)
print(f"Dopo rimozione nulls obbligatori: {df.count():,}")

# =============================================================
# 4. Esclusione voli dirottati (diverted = 1)
# =============================================================

df = df.filter(F.col("diverted") == 0).drop("diverted")
print(f"Dopo esclusione voli dirottati:   {df.count():,}")

# =============================================================
# 5. Gestione ritardi
#    - Voli operati (cancelled=0): dep_delay e arr_delay devono
#      essere presenti
#    - Voli cancellati (cancelled=1): i campi ritardo sono NULL
# =============================================================

df = df.filter(
    (F.col("cancelled") == 1) |
    (F.col("dep_delay").isNotNull() & F.col("arr_delay").isNotNull())
)
print(f"Dopo gestione ritardi:            {df.count():,}")

# =============================================================
# 6. Gestione cause di ritardo
#    - Volo operato: NULL → 0 (nessun ritardo per quella causa)
#    - Volo cancellato: lascia NULL
# =============================================================

delay_cause_cols = [
    "carrier_delay", "weather_delay", "nas_delay",
    "security_delay", "late_aircraft_delay"
]

for col_name in delay_cause_cols:
    df = df.withColumn(
        col_name,
        F.when(F.col("cancelled") == 0, F.coalesce(F.col(col_name), F.lit(0)))
         .otherwise(F.lit(None))
    )

# =============================================================
# 7. Validazione temporale: mese nel range 1-12
# =============================================================

df = df.filter(F.col("month").between(1, 12))
print(f"Dopo validazione temporale:       {df.count():,}")

# =============================================================
# 8. Normalizzazione stringhe (trim + uppercase)
# =============================================================

df = df.withColumn("op_unique_carrier", F.trim(F.upper(F.col("op_unique_carrier"))))
df = df.withColumn("origin",            F.trim(F.upper(F.col("origin"))))

# =============================================================
# 9. Mappatura codici cancellazione -> etichette leggibili
#    A = Carrier, B = Weather, C = NAS, D = Security
#    Codici non validi -> NULL; voli operati -> "Not Cancelled"
# =============================================================

valid_codes = ["A", "B", "C", "D"]

df = df.withColumn(
    "cancellation_code",
    F.when(F.col("cancellation_code").isin(valid_codes), F.col("cancellation_code"))
     .otherwise(F.lit(None))
)

df = df.withColumn(
    "cancellation_reason",
    F.when(F.col("cancellation_code") == "A", "Carrier")
     .when(F.col("cancellation_code") == "B", "Weather")
     .when(F.col("cancellation_code") == "C", "NAS")
     .when(F.col("cancellation_code") == "D", "Security")
     .otherwise("Not Cancelled")
)

# Correggi coerenza: voli operati non devono avere cancellation_code
df = df.withColumn(
    "cancellation_code",
    F.when(F.col("cancelled") == 0, F.lit(None))
     .otherwise(F.col("cancellation_code"))
)

print(f"Dopo mappatura cancellazioni:     {df.count():,}")

# =============================================================
# 10. Report qualità finale
# =============================================================

count_raw   = df_raw.count()
count_clean = df.count()

print("\n" + "="*50)
print(f"  Record originali:      {count_raw:>10,}")
print(f"  Record dopo pulizia:   {count_clean:>10,}")
print(f"  Record eliminati:      {count_raw - count_clean:>10,}  ({(count_raw - count_clean) / count_raw * 100:.1f}%)")
print("="*50)

print("\nDistribuzione per mese:")
df.groupBy("month").count().orderBy("month").show()

print("Top 10 compagnie per numero di voli:")
df.groupBy("op_unique_carrier").count().orderBy(F.desc("count")).show(10)

print("Top 10 aeroporti di partenza:")
df.groupBy("origin").count().orderBy(F.desc("count")).show(10)

# =============================================================
# 11. Salvataggio e generazione subset per i benchmark
#
#     Il dataset viene prima mescolato casualmente (seed fisso
#     per riproducibilita'), poi vengono estratte porzioni di
#     dimensione crescente e salvate nella cartella input/:
#
#       500k  ->  ~500.000 record
#       1M    ->  ~1.000.000 record
#       3M    ->  ~3.000.000 record
#       5M    ->  ~5.000.000 record
#       full  ->  intero dataset pulito
# =============================================================

def save_csv(df, path):
    """Salva un DataFrame Spark in un singolo file CSV."""
    tmp = path + "_tmp"
    df.coalesce(1).write.mode("overwrite") \
        .option("header", True).option("sep", ",").csv(tmp)
    for f in os.listdir(tmp):
        if f.endswith(".csv"):
            shutil.move(os.path.join(tmp, f), path)
            break
    shutil.rmtree(tmp)

os.makedirs("input", exist_ok=True)
SEED = 42

# Shuffle con seed fisso per garantire riproducibilita'
df_shuffled = df.orderBy(F.rand(seed=SEED))
total = df_shuffled.count()
print(f"\nRecord totali dopo shuffle: {total:,}")

subsets = [
    ("500k", 500_000),
    ("1M",   1_000_000),
    ("3M",   3_000_000),
    ("5M",   5_000_000),
    ("full", None),
]

for name, n in subsets:
    path = f"input/flight_{name}.csv"
    if n is not None:
        if n > total:
            print(f"  Salto {name}: richiesti {n:,} ma disponibili solo {total:,}.")
            continue
        df_subset = df_shuffled.sample(withReplacement=False, fraction=n / total, seed=SEED).limit(n)
    else:
        df_subset = df_shuffled

    save_csv(df_subset, path)
    print(f"  flight_{name}.csv  ->  {df_subset.count():,} record")

print("\n[SUCCESS] Tutti i subset salvati in input/")
spark.stop()