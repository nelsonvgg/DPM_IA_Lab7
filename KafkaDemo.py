import os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
from kafka import KafkaConsumer, KafkaProducer

# Update this for your own recitation section :)
topic = 'recitation-x' # x could be b, c, d, e, f"

# Producer Mode -> Writes Data to Broker"
# [TODO]: Replace '...' with the address of your Kafka bootstrap server\n",
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

# [TODO]: Add cities of your choice
cities = ["Bogota", "Quito", "Lima"]
#cities = ["London", "Berlin", "Paris", "Madrid", "Rome", "Tokyo"]

# Write data via the producer
print("Writing to Kafka Broker")
for i in range(10):
    data = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")},{cities[randint(0,len(cities)-1)]},{randint(18, 32)}ºC'
    print(f"Writing: {data}")
    producer.send(topic=topic, value=data)
    sleep(1)


# Consumer Mode -> Reads Data from Broker"
# [TODO]: Complete the missing ... parameters/arguments using the Kafka documentation\n",
consumer = KafkaConsumer(
    'recitation-x', # Topic name
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest", #Experiment with different values\n",
    # Commit that an offset has been read\n",
    enable_auto_commit=True,
    # How often to tell Kafka, an offset has been read\n",
    auto_commit_interval_ms=1000
)

print('Reading Kafka Broker')
for message in consumer:
    message = message.value.decode()
    # Default message.value type is bytes!
    print(loads(message))
    os.system(f"echo {message} >> kafka_log.csv")