import json
import random
import time
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

DB_URL = "postgresql://midas:midas123@localhost:5433/midasdb"
engine = create_engine(DB_URL)

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

for i in range(200):
    sender = random.randint(1, 20)
    recipient = random.randint(1, 20)
    while recipient == sender:
        recipient = random.randint(1, 20)
    
    transaction = {
        "senderId": sender,
        "recipientId": recipient,
        "amount": round(random.uniform(10, 500), 2)
    }
    producer.send("trader-updates", transaction)
    time.sleep(0.05)

producer.flush()
print("Sent 200 transactions via Kafka")