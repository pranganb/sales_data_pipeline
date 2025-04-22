import json
import random
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer

# Initialize Kafka producer
kafka_config = {
    'bootstrap.servers':'CLUSTER_CONNECTION_STRING',
    'security.protocol':'SASL_SSL',
    'sasl.mechanism':'PLAIN',
    'sasl.username':'YOUR_API_KEY',
    'sasl.password':'YOUR_API_SECRET_KEY',
    
}

producer = Producer(**kafka_config)

# Function to generate payment data
def generate_payment(order_id, payment_id):
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_date": str((datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()),
        "created_at": str(datetime.now().isoformat()),
        "amount": random.randint(50, 500)
    }

# Specify order_id and publish a single payment
order_id = "order_2"
payment_id = str(uuid.uuid4())

try:
    payment = generate_payment(order_id, payment_id)
    # serialise the payment
    serialized_payment = json.dumps(payment).encode('utf-8')
    producer.produce('payments_topic_data_v1', value=serialized_payment)
    print(f"Sent payment: {payment}")
finally:
    producer.flush()
    
