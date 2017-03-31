import pika
import time



credentials = pika.PlainCredentials('guest', '***REMOVED***')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='test-queue')

for i in range(6):
    channel.basic_publish(exchange='', routing_key='test-queue', body='randomshit')

def callback(ch, method, properties, dn):
    print "sleep 1 seconds for msg %r" % dn
    time.sleep(1)
    ch.basic_ack( delivery_tag = method.delivery_tag)

channel.basic_consume(callback, queue='test-queue',no_ack=False)
channel.start_consuming()
channel.close()


