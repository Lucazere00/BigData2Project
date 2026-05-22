#!/bin/bash
# run_benchmark_job2.sh - Job 2 MapReduce
# Esegue il Job 2 su Hadoop per dataset di dimensioni crescenti

MAPPER=$(pwd)/mapper_job2.py
REDUCER=$(pwd)/reducer_job2.py
HDFS_INPUT_DIR="/user/$USER/input"
HDFS_OUTPUT_DIR="/user/$USER/output_job2_mapreduce"
LOCAL_RESULTS_DIR="./output_job2_mapreduce"
LOG_FILE="benchmark_job2_mapreduce.txt"

HADOOP_STREAMING_JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -n 1)

mkdir -p $LOCAL_RESULTS_DIR

echo "Benchmark MapReduce - Job 2" > $LOG_FILE
echo "============================" >> $LOG_FILE
echo "Inizio: $(date)" >> $LOG_FILE
echo "" >> $LOG_FILE

for SIZE in 500k 1M 3M 5M full; do
    HDFS_INPUT_FILE="$HDFS_INPUT_DIR/flight_${SIZE}.csv"
    HDFS_OUTPUT_PATH="$HDFS_OUTPUT_DIR/$SIZE"
    LOCAL_OUTPUT_PATH="$LOCAL_RESULTS_DIR/$SIZE"

    echo "Esecuzione per input: $SIZE"
    hdfs dfs -rm -r "$HDFS_OUTPUT_PATH" 2>/dev/null

    START=$(date +%s)

    hadoop jar $HADOOP_STREAMING_JAR \
        -D mapreduce.job.reduces=1 \
        -files "$MAPPER,$REDUCER" \
        -mapper  "python3 mapper_job2.py" \
        -reducer "python3 reducer_job2.py" \
        -input   "$HDFS_INPUT_FILE" \
        -output  "$HDFS_OUTPUT_PATH"

    END=$(date +%s)
    DURATION=$((END - START))

    rm -rf "$LOCAL_OUTPUT_PATH"
    mkdir -p "$LOCAL_OUTPUT_PATH"
    hdfs dfs -get "$HDFS_OUTPUT_PATH/part-00000" "$LOCAL_OUTPUT_PATH/risultato_$SIZE.json"

    echo "$SIZE completato in $DURATION secondi"
    echo "$SIZE: $DURATION sec" >> $LOG_FILE
done

echo ""
echo "Tempi salvati in:     $LOG_FILE"
echo "Risultati salvati in: $LOCAL_RESULTS_DIR"