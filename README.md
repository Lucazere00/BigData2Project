# BigData2Project

Progetto per il corso di Big Data — Università degli Studi Roma Tre.  
Analisi comparativa di tecnologie per l'elaborazione di big data su un dataset reale di voli del 2024.

**Gruppo:** WN Group  
**Studenti:** Luca Zerella (631722), Gianluca De Musis()

---

## Struttura del progetto
```text
BIGDATA2PROJECT
├── aws_job1/
│   ├── aws_job1_spark_core.py
│   └── aws_job1_spark_sql.py
├── aws_job2/
│   ├── aws_job2_spark_core.py
│   └── aws_job2_spark_sql.py
├── HadoopStreamingJob1/
│   ├── output_job1_mapreduce/        ← generata dal benchmark
│   ├── benchmark_job1_mapreduce.txt  ← generato dal benchmark
│   ├── mapper_job1.py
│   ├── reducer_job1.py
│   └── run_benchmark_job1.sh
├── HadoopStreamingJob2/
│   ├── output_job2_mapreduce/        ← generata dal benchmark
│   ├── benchmark_job2_mapreduce.txt  ← generato dal benchmark
│   ├── mapper_job2.py
│   ├── reducer_job2.py
│   └── run_benchmark_job2.sh
├── input/                            ← generata da prepare_data.py
├── output_job1_sparkcore/            ← generata da job1_spark_core.py
├── output_job1_sparksql/             ← generata da job1_spark_sql.py
├── output_job2_sparkcore/            ← generata da job2_spark_core.py
├── output_job2_sparksql/             ← generata da job2_spark_sql.py
├── SparkJob1/
│   ├── job1_spark_core.py
│   └── job1_spark_sql.py
├── SparkJob2/
│   ├── job2_spark_core.py
│   └── job2_spark_sql.py
├── flight_data_2024.csv              ← da aggiungere manualmente (non incluso nel repo)
├── prepare_data.py
└── README.md
...
```
---

## Dataset

Il dataset utilizzato è il **[Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)**, disponibile su Kaggle.

Scaricare il file CSV e posizionarlo nella **root del progetto** con il nome esatto:

flight_data_2024.csv

Il dataset non è incluso nel repository per via delle sue dimensioni (oltre 7 milioni di righe).

---


## Riproduzione delle soluzioni

### Step 1 — Preparazione del dataset

Eseguire lo script di preparazione dalla **root del progetto**:

```bash
python prepare_data.py
```

Questo script:
- Carica `flight_data_2024.csv`
- Applica le operazioni di pulizia e normalizzazione
- Genera la cartella `input/` con i seguenti subset:
```text
input/
├── flight_500k.csv
├── flight_1M.csv
├── flight_3M.csv
├── flight_5M.csv
└── flight_full.csv
...
```
---

### Step 2 — Esecuzione dei job Spark in locale

Dalla **root del progetto**, eseguire i job nell'ordine preferito:

**Job 1:**

```bash
python SparkJob1/job1_spark_sql.py
python SparkJob1/job1_spark_core.py
```

Output generato in:
output_job1_sparksql/
output_job1_sparkcore/

**Job 2:**

```bash
python SparkJob2/job2_spark_sql.py
python SparkJob2/job2_spark_core.py
```

Output generato in:
```text
output_job2_sparksql/
output_job2_sparkcore/
...

Ogni script elabora automaticamente tutti e 5 i subset e salva i risultati in formato JSON.

---

### Step 3 — Esecuzione dei job MapReduce (Hadoop Streaming)

**Avviare Hadoop:**

```bash
start-dfs.sh
start-yarn.sh
```

**Caricare i file CSV su HDFS:**

```bash
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -put input/flight_*.csv /user/$USER/input/
```

**Eseguire i benchmark:**

```bash
cd HadoopStreamingJob1
bash run_benchmark_job1.sh

cd ../HadoopStreamingJob2
bash run_benchmark_job2.sh
```

I risultati vengono scaricati automaticamente da HDFS in:
```text
HadoopStreamingJob1/output_job1_mapreduce/
HadoopStreamingJob2/output_job2_mapreduce/
...
```
