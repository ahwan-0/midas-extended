import json
import random
from datetime import datetime, timedelta
import time
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

DB_URL = "postgresql://midas:midas123@localhost:5433/midasdb"
engine = create_engine(DB_URL)

NUM_USERS = 20
NUM_TRANSACTIONS = 5000

# First insert users directly into DB
users = [
    {"id": i, "name": f"User_{i}", "balance": round(random.uniform(1000, 10000), 2)}
    for i in range(1, 21)
]

with engine.connect() as conn:
    conn.execute(text("TRUNCATE user_record RESTART IDENTITY CASCADE"))
    for u in users:
        conn.execute(text(
            "INSERT INTO user_record (name, balance) VALUES (:name, :balance)"
        ), {"name": u["name"], "balance": u["balance"]})
    conn.commit()
print("Inserted 20 users")

# Now send transactions via Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

base_time = datetime(2026, 1, 1, 9, 0, 0)

velocity = {}

for i in range(NUM_TRANSACTIONS):
    if random.random() < 0.10:
        hour = random.choice([2, 3, 4])
    else:
        hour = random.randint(9, 18)
    timestamp = base_time + timedelta(days=i//50, hours=hour, minutes=random.randint(0,59))

    sender = random.randint(1, 20)
    recipient = random.randint(1, 20)
    while recipient == sender:
        recipient = random.randint(1, 20)
    amount = round(random.uniform(10, 500), 2)

    flags = 0
    if amount > 450:                    flags += 1
    if amount % 100 < 1:               flags += 1
    if hour in [2, 3, 4]:              flags += 1
    if velocity.get(sender, 0) > 4:    flags += 1
    is_fraud = 1 if flags >= 2 else 0

    velocity[sender] = velocity.get(sender, 0) + 1

    transaction = {
        "senderId": sender,
        "recipientId": recipient,
        "amount": amount,
        "is_fraud": is_fraud,
        "timestamp": timestamp.isoformat(),
    }
    producer.send("trader-updates", transaction)

producer.flush()
print(f"Sent {NUM_TRANSACTIONS} transactions via Kafka")