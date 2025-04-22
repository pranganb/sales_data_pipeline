import json
import random
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

# Initialize Kafka producer
kafka_config = {
    'bootstrap.servers':'YOUR_CLUSTER_CONNECTION_STRING',
    'security.protocol':'SASL_SSL',
    'sasl.mechanism':'PLAIN',
    'sasl.username':'YOUR_API_KEY',
    'sasl.password':'YOUR_API_SECRET',
    
}

producer = Producer(**kafka_config)

# Function to generate order data
def generate_order(order_id):
    return {
        "order_id": order_id,
        "order_date": str((datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()),
        "created_at": str(datetime.now().isoformat()),
        "customer_id": f"customer_{random.randint(1, 100)}",
        "amount": random.randint(100, 1000)
    }

# Publish orders with duplicates
try:
    order_id_counter = 1
    for _ in range(20):
        order_id = f"order_{order_id_counter}"
        order = generate_order(order_id)

        
        # Serialize the order manually
        serialized_order = json.dumps(order).encode('utf-8')

        # Send the serialized order
        producer.produce('orders_topic_data_v1', value=serialized_order)
        print(f"Sent order: {order}")

        # Randomly duplicate the same order
        if random.choice([True, False]):
            producer.produce('orders_topic_data_v1', value=serialized_order)
            print(f"Sent duplicate order: {order}")

        order_id_counter += 1
        time.sleep(6)
finally:
    producer.flush()
    
