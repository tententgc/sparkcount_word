from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, window, avg, min, max, count, sum
from pyspark.sql.types import StructType, StringType, FloatType, LongType, ArrayType

# สร้าง Spark Session
spark = SparkSession.builder \
    .appName("StockPriceStreamingAggregator") \
    .master("local[*]") \
    .getOrCreate() 

spark.sparkContext.setLogLevel("WARN")

# Define schema for a single stock entry
stock_schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("timestamp", LongType())

# Kafka message = list of stock records → use ArrayTyps
message_schema = ArrayType(stock_schema)

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Decode and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), message_schema).alias("stocks"))

# Flatten the array of stocks (explode)
df_flat = df_parsed.select(explode("stocks").alias("stock")) \
    .select(
        col("stock.symbol"),
        col("stock.price"),
        col("stock.high"),
        col("stock.low"),
        col("stock.timestamp").cast("timestamp").alias("event_time")
    )

agg_df = df_flat.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),col("symbol")
    # window(col("event_time"), "3 minutes"),col("symbol")
).agg(
    count("price").alias("count"),
    sum("price").alias("sum"),
    avg("price").alias("mean"),
    min("price").alias("min"),
    max("price").alias("max")
).select(
    col("window.start").alias("start"),
    col("window.end").alias("end"),
    "symbol", "count", "mean", "min", "max"
).orderBy("symbol","start")


query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
