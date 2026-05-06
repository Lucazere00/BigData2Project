import pandas as pd
import os


INPUT_FILE = "flight_data_2024.csv"
OUTPUT_FILE = "flight_data_2024_clean.csv"


#1 Carico il dataset
print("Caricamento dataset...")
df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"      Righe iniziali: {len(df):,}")
print(f"      Colonne: {list(df.columns)}")


#2 Seleziono solo le colonne necessarie
print("Selezione colonne di interesse...")
COLUMNS = [
    "year", 
    "month",
    "day_of_month",
    "fl_date",
    "op_unique_carrier",         # codice compagnia aerea
    "origin",                    # aeroporto di partenza (codice IATA)
    "origin_city_name",          # città di partenza
    "dest",                      # aeroporto di destinazione
    "dest_city_name",            # città di destinazione
    "dep_delay",                 # ritardo partenza (minuti)
    "arr_delay",                 # ritardo arrivo (minuti)
    "cancelled",                 # volo cancellato (0/1)
    "cancellation_code",         # causa cancellazione (A/B/C/D)
    "diverted",                  # volo dirottato (0/1)
    "carrier_delay",             # ritardo causa compagnia
    "weather_delay",             # ritardo causa meteo
    "nas_delay",                 # ritardo causa NAS
    "security_delay",            # ritardo causa sicurezza
    "late_aircraft_delay",       # ritardo causa aereo in ritardo
    "distance",                  # distanza in miglia
    "crs_elapsed_time",          # durata prevista
    "actual_elapsed_time",       # durata reale
]
df = df[COLUMNS]
print(f"      Colonne selezionate: {len(COLUMNS)}")


#3 Eliminazione righe con dati mancanti nelle colonne di interesse
print("Eliminazione righe con valori nulli nelle colonne di interesse...")
rows_before = len(df)
key_columns = ["op_unique_carrier", "origin", "dest", "month", "year", "fl_date"]
df = df.dropna(subset=key_columns)
print(f"      Righe rimosse: {rows_before - len(df):,}")
 

#4 Normalizzazione

# Cast numerici
df["dep_delay"] = pd.to_numeric(df["dep_delay"], errors="coerce")
df["arr_delay"] = pd.to_numeric(df["arr_delay"], errors="coerce")
df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
df["cancelled"] = pd.to_numeric(df["cancelled"], errors="coerce").fillna(0).astype(int)
df["diverted"] = pd.to_numeric(df["diverted"], errors="coerce").fillna(0).astype(int)
df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

# Colonne cause del ritardo: riempie NaN con 0 (volo non cancellato = nessun ritardo per quella causa)
delay_columns = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]
for col in delay_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
 