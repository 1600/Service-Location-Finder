import pika
import time

qn = 'test'
credentials = pika.PlainCredentials('guest', '***REMOVED***')

def callback(ch, method, properties, num):
    #time.sleep(1)
    if int(num) %2 == 0:
        ch.basic_ack( delivery_tag = method.delivery_tag)
    print num

connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue=qn)
channel.basic_consume(callback, queue=qn, no_ack=False)
channel.start_consuming()


