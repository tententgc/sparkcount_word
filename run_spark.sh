#!/bin/bash

echo "[🚀] Starting Spark Streaming App with Kafka Connector..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_streaming_app.py