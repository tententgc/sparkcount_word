import logging
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp

# ปิด log info
logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)

spark = SparkSession.builder \
    .appName("JSONReportKafkaStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "finnhub") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as message")

df_with_timestamp = df.withColumn("timestamp", current_timestamp())
    
words = df_with_timestamp.select(
    explode(split(df_with_timestamp.message, " ")).alias("word"),
    df_with_timestamp.timestamp
)
word_counts = words.groupBy(
    window("timestamp", "60 seconds", "10 seconds"),
    "word"
).count()


query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()
    
# while query.isActive:
#     time.sleep(10)
#     progress = query.lastProgress
#     if progress:
#         print(json.dumps(progress, indent=2))
#     with open("report.json", "w") as f:
#         json.dump(progress, f, indent=2)
        
        
query.awaitTermination()