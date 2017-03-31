#!coding=utf-8
import pika
import json
import time
import rethinkdb as r
from store_to_queue import Publisher

def callback(ch, method, properties, rec):
    try:
        time.sleep(0.2)
        a = json.loads(rec)
    except:
        print 'json loads error processing >>',rec
        return
    try:
        pre_insert = a['data']['data'][1]
    except Exception as e:
        print 'pre_insert index error >>',a
        return
    try:
        msg = r.table('New_Domain_Location').insert(pre_insert).run(conn)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    except:
        print 'insert error >>',msg
        return

    



if __name__ == '__main__':
    #pub = Publisher('')
    conn = r.connect(host='172.16.1.2', db='IP')
    credentials = pika.PlainCredentials('guest', '***REMOVED***')

    qn = 'ip-location-to-insert'
    def start_consume():
        connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue=qn)
        #channel.basic_consume(callback, queue=qn, no_ack=False)
        channel.basic_consume(callback, queue=qn, no_ack=False)
        channel.start_consuming()

    while True:
        time.sleep(2)                       # wait 2 seconds before reconnect
        print '[x] Starting a round of pika processing.'
        try:
            start_consume()
        except Exception as e:
            print "consuming Exception >>",e
