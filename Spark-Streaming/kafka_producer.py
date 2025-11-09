# Pull and run Apache Kafka ina Docker Container 

# docker pull apache/kafka:4.1.0
# docker run -p 9092:9092 apache/kafka:4.1.0

# Then install Python Package confluent_kafka
# pip3 install confluent_kafka

from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print("Delivered to", msg.topic(), msg.partition())

for i in range(5):
    producer.produce("test_topic", key=str(i), value=f"message {i}", callback=delivery_report)

producer.flush()

