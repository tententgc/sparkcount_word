# Real-Time Financial Word Count with PySpark & Kafka

This project streams financial data from [Finnhub](https://finnhub.io), sends it to Kafka, and uses PySpark Structured Streaming with a sliding window to count word frequency in real-time.

---

## 🧱 Project Structure

```
.
├── run_spark.sh              # Entry point to run Spark Streaming
├── finnhub_to_kafka.py       # Finnhub client pushes news to Kafka
├── spark_streaming_app.py    # PySpark consumer with sliding window word count
├── .env                      # Configuration file for secrets
└── README.md
```

---

## ⚙️ Prerequisites

- Python 3.10+
- Apache Kafka
- Apache Spark 3.5.1
- Java 11
- MacOS (tested on M4 chip)
- Install Python packages:

```bash
pip install -r requirements.txt
```

```text
pyspark
kafka-python
python-dotenv
```

---

## 🔐 .env Configuration

Create a `.env` file in the root directory: api_key from finnhub

```env
FINNHUB_API_KEY=your_finnhub_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=finnhub
```

---

## ▶️ How to Run

### 1. Start Kafka
```bash
# Start Zookeeper and Kafka (if using Homebrew)
brew services start zookeeper
brew services start kafka
```

### 2. Run Finnhub Kafka Producer
```bash
python finnhub_to_kafka.py
```

### 3. Run Spark Streaming Job
```bash
bash run_spark.sh
```

This will launch `spark_streaming_app.py` and connect to the Kafka topic using structured streaming.

---

## 🧾 Output Example

```
-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+----------+-----+
|window              |word      |count|
+--------------------+----------+-----+
|{...}               |ETF       |2    |
...
+--------------------+----------+-----+
only showing top 20 rows
```

---

## 🛠️ Customization

If you want to save output to JSON, modify the `writeStream` sink:

```python
.writeStream.outputMode("complete").format("json").option("path", "output/").start()
```

---

## 📉 Log Noise Reduction

To reduce Spark log verbosity, set log level in `spark_streaming_app.py`:

```python
spark.sparkContext.setLogLevel("ERROR")
```

---

## ✅ Done!
You now have a real-time data pipeline from Finnhub → Kafka → PySpark with word count over sliding windows.