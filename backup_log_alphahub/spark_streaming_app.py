from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# สร้าง SparkContext และ StreamingContext (batch interval = 10s)
sc = SparkContext("local[2]", "FinancialDataSlidingWindow")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint_dir")

# สร้าง DStream ที่อ่านจาก socket
lines = ssc.socketTextStream("localhost", 9999)

def parse_json(line):
    try:
        # รับ list ของ dicts จาก server
        records = json.loads(line)
        # ถ้าเป็น dict เดียว (เช่นตอนทดสอบ) ก็ห่อให้เป็น list
        if isinstance(records, dict):
            return [records]
        elif isinstance(records, list):
            return records
        else:
            return []
    except Exception as e:
        print("Parse error:", e)
        return []

# flatMap แปลงเป็น record เดี่ยวต่อบรรทัด
records = lines.flatMap(parse_json)

# Map แต่ละ record เป็น (symbol, 1)
symbol_counts = records.map(lambda record: (record['symbol'], 1))

# ใช้ sliding window (window 60s, slide 10s)
windowed_counts = symbol_counts.reduceByKeyAndWindow(
    lambda x, y: x + y,
    lambda x, y: x - y,
    windowDuration=60,
    slideDuration=20
)

# Print ผลลัพธ์ทุก window
windowed_counts.pprint()

ssc.start()
ssc.awaitTermination()
