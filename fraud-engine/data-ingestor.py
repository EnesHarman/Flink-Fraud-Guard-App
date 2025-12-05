# datagen.py
import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

print("Starting Transaction Generator...")

while True:
    # Simulate a user
    user_id = f"User-{random.randint(1, 5)}" # Small pool to ensure collisions for state testing

    if random.randint(0, 100) < 10:
        print("!!! SPAMMING !!!")
        spam_user = f"User-{random.randint(1, 5)}"
        for _ in range(8): # Send 8 transactions rapidly
            tx = {
                "transactionId": fake.uuid4(),
                "userId": spam_user,
                "amount": 1.00,
                "timestamp": int(time.time() * 1000),
                "lat": float(fake.latitude()),
                "lon": float(fake.longitude())
            }
            producer.send('transactions', value=tx)

    transaction = {
        "transactionId": fake.uuid4(),
        "userId": user_id,
        "amount": round(random.uniform(5.0, 500.0), 2),
        "timestamp": int(time.time() * 1000),
        "lat": float(fake.latitude()),
        "lon": float(fake.longitude())
    }

    print(f"Sending: {transaction}")
    producer.send('transactions', value=transaction)
    time.sleep(1) # 1 transaction per second for now