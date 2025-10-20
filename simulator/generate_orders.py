import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

faker = Faker()

# Azure Event Hub settings
BROKER = '<<namespace_host_name>>:9093'
EVENT_HUB = '<<event_hub_name>>'
CONNECTION_STRING = '<<namespace_connection_string>>'

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda val: json.dumps(val).encode('utf-8')
)

product_categories = ['Electronics', 'Books', 'Clothing', 'Home Decor', 'Toys']
us_locations = [
    {"city": "San Francisco", "state": "CA", "lat": 37.7749, "lon": -122.4194},
    {"city": "Miami", "state": "FL", "lat": 25.7617, "lon": -80.1918},
    {"city": "Seattle", "state": "WA", "lat": 47.6062, "lon": -122.3321},
    {"city": "Denver", "state": "CO", "lat": 39.7392, "lon": -104.9903},
    {"city": "Boston", "state": "MA", "lat": 42.3601, "lon": -71.0589}
]

def create_order():
    loc = random.choice(us_locations)
    category = random.choice(product_categories)
    price = round(random.uniform(15, 2500), 2)
    qty = random.randint(1, 5)

    return {
        "order_id": faker.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": faker.uuid4(),
        "product_id": faker.uuid4(),
        "category": category,
        "price": price,
        "quantity": qty,
        "total_amount": round(price * qty, 2),
        "city": loc["city"],
        "state": loc["state"],
        "country": "USA",
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "delivery_status": random.choice(["Processing", "Shipped", "Delivered", "Cancelled"])
    }

if __name__ == "__main__":
    print("Sending simulated U.S. e-commerce order data to Azure Event Hub...")
    while True:
        record = create_order()
        producer.send(EVENT_HUB, value=record)
        print("Sent:", record)
        time.sleep(2)
