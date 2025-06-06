from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, count, approx_count_distinct
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0 pyspark-shell'

schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("from_bank", StringType()) \
    .add("from_account", StringType()) \
    .add("to_bank", StringType()) \
    .add("to_account", StringType()) \
    .add("amount_received", DoubleType()) \
    .add("receiving_currency", StringType()) \
    .add("amount_paid", DoubleType()) \
    .add("payment_currency", StringType()) \
    .add("payment_format", StringType()) \
    .add("is_laundering", IntegerType())
    
spark = SparkSession.builder \
    .appName("AMLTransactionStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aml_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. แปลง value จาก json → column
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 3. สร้าง event_time จาก timestamp ปัจจุบัน (หรือ field ใน message ก็ได้)
from pyspark.sql.functions import to_timestamp
parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))

# 4. ดู raw output ก่อน (Debug)
raw_query = parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# 5. Sliding Window Aggregation (1 min window, slide ทุก 30 วิ)
agg = parsed \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("Payment Format")
    ).agg(
        {"Amount Received": "sum", "Amount Paid": "sum", "*": "count"}
    ) \
    .withColumnRenamed("sum(Amount Received)", "total_income") \
    .withColumnRenamed("sum(Amount Paid)", "total_outcome") \
    .withColumnRenamed("count(1)", "num_transactions")

# 6. Print result
agg_query = agg.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

agg_query.awaitTermination()