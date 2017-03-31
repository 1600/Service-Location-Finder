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
    qn = 'test'

    worker = Publisher(qn)
    worker.connect()

    for i in xrange(10):
        worker.publish(json.dumps(i))
        
    worker.close()

# r.db('IP').table('Domain_Location').filter({'country_id':'US'}).filter({'RefDomainType':0}).count()   # 101164