import finnhub
from kafka import KafkaProducer
import json
import time
from dotenv import load_dotenv
import os
import finnhub


load_dotenv()


finnhub_api_key = os.getenv("FINNHUB_API_KEY")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)

# สร้าง Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    try:
        news = finnhub_client.general_news('general', min_id=0)
        for item in news[:5]:  # ส่งเฉพาะ 5 ข่าวล่าสุด
            text = item.get('headline', '')
            producer.send("finnhub", value=text)
            print(f"[Kafka] {text}")
        time.sleep(10)  # ทุก 10 วินาที
    except Exception as e:
        print("Error:", e)
        time.sleep(5)
