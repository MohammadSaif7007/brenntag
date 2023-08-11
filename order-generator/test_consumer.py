from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['orders'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        print(msg)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
