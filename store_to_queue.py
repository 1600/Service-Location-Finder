# -*- coding: utf-8 -*-
import logging
import json
import pika
import rethinkdb as r


class Publisher:
    EXCHANGE=''

    def __init__(self,queueName):
        username = 'guest'
        password = '***REMOVED***'
        MQ_host = '192.168.4.249'
        credentials = pika.PlainCredentials(username, password)
        self.ROUTING_KEY = queueName
        self._params = pika.ConnectionParameters(MQ_host, 5672, '/', credentials)
        self._conn = None
        self._channel = None

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.queue_declare(queue=self.ROUTING_KEY)

    def _publish(self, msg):
        self._channel.basic_publish(exchange=self.EXCHANGE, routing_key=self.ROUTING_KEY, body=msg)
        logging.debug('message sent: %s', msg)

    def publish(self, msg):
        """Publish msg, reconnecting if necessary."""
        try:
            self._publish(msg)
        except pika.exceptions.ConnectionClosed:
            logging.debug('reconnecting to queue')
            self.connect()
            self._publish(msg)

    def close(self):
        if self._conn and self._conn.is_open:
            logging.debug('closing queue connection')
            self._conn.close()



if __name__ == "__main__":
    qn = 'yilou-queue'
    worker = Publisher(qn)
    worker.connect()
    conn = r.connect(host='172.16.1.2', db='IP')
### choose what to store
    '''从文件中读取所有域名并写入队列
    '''
    with open('yilou.txt', 'r') as f:
        for i in f.readlines():
            dn = i[:-1]
            worker.publish(dn)

    # '''从数据库中读取所有美国IP并写入队列
    # '''
    # ss = r.table('Domain_Location').filter( lambda doc:doc['country_id']==u'US' ).filter({'RefDomainType':0}).run(conn)   
    # for i in ss:
    #     worker.publish(json.dumps(i))


### choose what to store

    worker.close()

# r.db('IP').table('Domain_Location').filter({'country_id':'US'}).filter({'RefDomainType':0}).count()   # 101164