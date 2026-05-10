#!/bin/bash
# run_benchmark.sh - Job 1 MapReduce
# Esegue il Job 1 su Hadoop per dataset di dimensioni crescenti

MAPPER=/home/lucazere00/Scrivania/BigData2Project/HadoopStreamingJob1/mapper.py
REDUCER=/home/lucazere00/Scrivania/BigData2Project/HadoopStreamingJob1/reducer.py
HDFS_INPUT_DIR="/user/lucazere00/input"
HDFS_OUTPUT_DIR="/user/lucazere00/output_job1_mapreduce"
LOCAL_RESULTS_DIR="./output_job1_mapreduce"
LOG_FILE="benchmark_job1_mapreduce.txt"

# Crea cartella locale per i risultati
mkdir -p $LOCAL_RESULTS_DIR

# Reset log
echo "Benchmark MapReduce - Job 1" > $LOG_FILE
echo "============================" >> $LOG_FILE

HADOOP_STREAMING_JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

for SIZE in 500k 1M 3M full; do
    HDFS_INPUT_FILE="$HDFS_INPUT_DIR/flight_${SIZE}.csv"
    HDFS_OUTPUT_PATH="$HDFS_OUTPUT_DIR/$SIZE"
    LOCAL_OUTPUT_PATH="$LOCAL_RESULTS_DIR/$SIZE"

    echo "▶️  Esecuzione per input: $SIZE"

    # Rimuovi output precedente su HDFS
    hdfs dfs -rm -r "$HDFS_OUTPUT_PATH" 2>/dev/null

    # Misura tempo
    START=$(date +%s)

    hadoop jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.reduces=1 \
        -files "$MAPPER,$REDUCER" \
        -mapper  "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -input  "$HDFS_INPUT_FILE" \
        -output "$HDFS_OUTPUT_PATH"

    END=$(date +%s)
    DURATION=$((END - START))

    # Scarica risultato da HDFS (rimuove prima il vecchio)
    echo "📥 Scaricamento risultati per $SIZE..."
    rm -rf "$LOCAL_OUTPUT_PATH"
    mkdir -p "$LOCAL_OUTPUT_PATH"
    hdfs dfs -get "$HDFS_OUTPUT_PATH/part-00000" "$LOCAL_OUTPUT_PATH/risultato_$SIZE.json"

    echo "✅ $SIZE completato in $DURATION secondi"
    echo "$SIZE: $DURATION sec" >> $LOG_FILE
done

echo ""
echo "📊 Tempi salvati in: $LOG_FILE"
echo "📂 Risultati salvati in: $LOCAL_RESULTS_DIR"