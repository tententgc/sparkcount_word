# finnhub_price_producer.py
import finnhub
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Setup Finnhub client
finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Stock symbols to track
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

while True:
    try:
        financial_data = []
        timestamp = int(time.time())

        for symbol in symbols:
            quote = finnhub_client.quote(symbol)
            data = {
                'symbol': symbol,
                'price': quote.get('c'),
                'high': quote.get('h'),
                'low': quote.get('l'),
                'timestamp': timestamp
            }
            financial_data.append(data)
            print(f"📈 {symbol}: {data['price']} (High: {data['high']}, Low: {data['low']})")
            time.sleep(1)  # avoid rate limit

        # Send one message with all stocks
        producer.send('stock_prices', value=financial_data)
        print("✓ Sent batch data for all symbols.")
        producer.flush()

        print("⏳ Waiting 60 seconds...\n")
        time.sleep(60)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(30)
