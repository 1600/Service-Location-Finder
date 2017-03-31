import pika
credentials = pika.PlainCredentials('guest', '***REMOVED***')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='test')

for i in range(0,10):
    channel.basic_publish(exchange='', routing_key='test', body='trash')


