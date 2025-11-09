from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "test-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["test_topic"])

print("Listening...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    print("Received:", msg.key(), msg.value())

