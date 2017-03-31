import pika

def callback(ch, method, properties, rec):
    print 111
    ch.basic_reject( delivery_tag = method.delivery_tag)
    print 222
    ch.basic_ack( delivery_tag = method.delivery_tag)
    print 333
    
    
    

credentials = pika.PlainCredentials('guest', '***REMOVED***')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='test')
channel.basic_consume(callback, queue='test', no_ack=False)
channel.start_consuming()


