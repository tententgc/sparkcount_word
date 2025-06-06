import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime 

# Settings
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'aml_transactions'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV
df = pd.read_csv('LI-Small_Trans.csv')
print(f"Listening on {KAFKA_BROKER} for topic {TOPIC}") 

for idx, row in df.iterrows():
    msg = row.to_dict()
    msg['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if 'Timestamp' in msg:
        del msg['Timestamp']
    producer.send(TOPIC, msg)
    print(f"Sent: {msg}")
    time.sleep(0.2)  # Simulate 1 transaction/sec streaming

producer.flush()
producer.close()
