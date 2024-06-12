from kafka import KafkaConsumer
import sqlite3

consumer = KafkaConsumer('test-topic', bootstrap_servers=['kafka:9092'])

conn = sqlite3.connect('/app/kafka_messages.db')
c = conn.cursor()

for message in consumer:
    print("Received message:", message.value.decode('utf-8'))

    c.execute("INSERT INTO kafka_messages (message) VALUES (?)", (message.value,))
    conn.commit()

consumer.close()
conn.close()
