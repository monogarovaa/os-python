from kafka import KafkaProducer
import time
import sqlite3

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

conn = sqlite3.connect('/app/kafka_messages.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS kafka_messages
             (message TEXT)''')
conn.commit()

for i in range(10):
    message = "Message {}".format(i).encode('utf-8')
    producer.send('test-topic', value=message)
    print("Sent message:", message)

    c.execute("INSERT INTO kafka_messages (message) VALUES (?)", (message,))
    conn.commit()
    time.sleep(1)

producer.close()
conn.close()
