# -*- coding: utf-8 -*-
# TODO : insert MQ to database
import json
import pika
import time
import gevent
import requests
import tldextract
import threading
import rethinkdb as r
from store_to_queue import Publisher
from gevent import monkey
from gevent.pool import Pool
import Queue
monkey.patch_all()

# rethinkdb config
conn = r.connect(host='172.16.1.2', db='IP')

# MQ config
err_q_name = 'error-domain-names'
err_q = Publisher(err_q_name)
err_q.connect()
to_insert_q_name = 'ip-location-to-insert'
to_insert_q = Publisher(to_insert_q_name)
to_insert_q.connect()

# requests config
session = requests.Session()
session.trust_env = False
headers = {
    'Host': 'mac.console.aliyun.com',
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Referer': 'https://mac.console.aliyun.com/httpdns',
    'Cookie': 'UM_distinctid=15ac5f8d2a03-005ffdaad24be2-5b123112-100200-15ac5f8d2a13f1; channel=W4x0IkTg1jQi1lpY0MQMjg%3D%3D; aliyun_choice=CN; industry-tip=true; consoleNavVersion=1.4.48; SLBNEW_JSESSIONID=8N566391-P1IJUUZDN3GUG0M5VMDU2-4M12NU0J-EL4; _slbnew_session0=qEN%2B4%2F9FDRR46%2FNmKAZORajtegzs0X7ZHV3mYUEC6%2BYpdJ7%2FAk1Xt2DIAtN3JDrglv5hk1ZEY9DDYv5caig2aSUJ2YyqHoxmMfpl0%2FnXO5b7hK3s5FDnzlyua5TEoTpZNAAY9qDaUIHvWCmGPlnyuvFR42mGScdEEsgkxKsyuh0AE6y%2BeeMZnz%2F9rHC0b%2FGzB6RS8tjIt3RVJLCPKkJTPcKlO1BbP1kw82DIHC8ugNX1vG%2B3VRPh8qDBmqzJXzZaFufKqZ39fGWGAkUdzapPmtB4t9B9tl3RU69Wj8OEc8uuuGyZmdcjd4SIWVmFkkebkZnokEPxQHHNuzgu7RWFjQ%3D%3D; COMMONBUY_JSESSIONID=T1666A81-X3BJR9Y9NI46A31FSBZB2-BG62NU0J-ACB; _commonbuy_session0=qEN%2B4%2F9FDRR46%2FNmKAZORZgKmbUfY0IsvLGivYWGYe9%2F2bZF0ufEe6SyOal8U2%2BO2itwnYwI9JZ7lHip%2BJGmTWQxT7Ou075WnUX0YYuoa%2BRJGimo9F%2F01z5FIHg8ykRiapK9v1tKtZE%2Flo20mmDpEMptcq1cMqEw57RU%2FF3KCHhYtvnxSn51CEGlHq04JhI1QAf0h75IDHFyX2lgqFbKevD3wHp%2FR3nhTIbRikdYdnG5OGolnZpPY5A0LNehgQ%2BHMYPy2btp58uhvWGoZvIsuYCX7pXZlq8uEuJokPICQPzU6r3zs6ni%2FJaCxlwEXASvPTmaAEeI4oXil344%2BbsOKQ%3D%3D; cna=29Y6EYeZFmkCAbeB2umWKlbE; _ga=GA1.2.1400381807.1471927483; login_aliyunid="***REMOVED***"; login_aliyunid_ticket=d4fWaCmBZHIzsgdZq64XXWQgyKFeuf0vpmV*s*CT58JlM_1t$w3eB$e*FK5yWzNSlN67azXMud7nGax1uGxfePTKl*PM5_g6pof_BNTwUhTOoNC1ZBeeMfKJzxdnb95hYssNIZor6q7SCxRtgmGCbi2G0; login_aliyunid_csrf=_csrf_tk_1399290837259248; ***REMOVED***; hssid=1z4oT95aVqDOJBhpgp1rsiA1; hsite=6; aliyun_country=CN; aliyun_site=CN; JSESSIONID=ZM5662B1-DAKJFXINTR482TD33UMM1-CMG0QV0J-V2; console0=BPBdD07Tvdk1xD9U4NnzYfik6OBTtvs73QRhqDkbdQxznG5kgpGNDZPRciTIEiRAW5lQBaLnAMd3EtZi2LmseeAvEA8fha609Vif3QboLy4E1ieCbF7gGFkBJMefggMD1EysINyGLA6BH1pyW6VYWKy0sf9Lde7ab5MLTHey%2FCDbZrMstQLQX9Jeh%2BlzH2HZJjm1Y1Y8KESDB2xghYvKHVzzSb6y66M%2B8QUDfPm2AuBAXbqwuSlboIjSNjjU4tvLnlMtlM%2Fm%2F43OPqFzAtzvb2RLV5EYml7c6DtBdY%2BQHHmGj9PkIGB5Dl0hICJ23c7FAe56cgf%2FWocEq5AaCkuL%2Bw%3D%3D; _HOME_JSESSIONID=3N566SA1-BPWIU7C3NONQFNWK13EX1-XKG0QV0J-SPRB; _home_session0=NTA04qTd%2Bi4xjQB3G%2FIAwSPxiWCUyIldGVk9%2B62bxCGvz1Yv6noa33otWpRXqume6b4CvvjggOctfut4FTay5wWwgQdOu2wk5mv9gTGSYrjSdkwWt%2BoBGp9lUiXGWxnj57PAbqTsmZbBKWuHLEvTj2nrsyhGnjuEcugP5cjbsW0f5RF7drYnslJB7kAHX5uJDXfHqAr1%2BJj6JrRUjACbLkN4MP5wpUb0x0384bTUUfvUMjd5l6Y1%2BIXg0E8B6wu9wzgr8IPgH25OPYbH%2F%2BwzrQGGMXULQheLBivHkIawD5VtnPJKSQWbI%2BQ7e5jfUZhAe0GTbK2y7yeLAkF5j8tWwg%3D%3D; consoleRecentVisit=dns%2Cslb; l=ArGxbgh1MA-hSKpiowDFdFhdQTdLbyUQ; isg=AkJCOPLdrsF3sL3zOpHk0Vjgk0iRDV6w6yGaJ4xbn7Vg3-FZeaLrPO0Z-WxZ',
}


def api(dn):
    r = session.get(
        'https://mac.console.aliyun.com/httpdns/resolveDomain.json?domain=' + dn, headers=headers)
    try:
        dic = json.loads(r.text)
        return dic
    except:
        print '[x] API ERROR ->' + dn + '<- api raw_response >>', r.text
        return


def callback(ch, method, properties, dn):
    ext = tldextract.extract(dn)
    dn_lvl2_dn = ext.domain + '.' + ext.suffix
    dn_prefix_www = 'www.' + ext.domain + '.' + ext.suffix
    try:
        dic = api(dn)
        if len(dic['data']['data']) == 1 or dic['data']['data'][1]['loc'] == u'美国':
            dic = api(dn_lvl2_dn)
            if len(dic['data']['data']) == 1 or dic['data']['data'][1]['loc'] == u'美国':
                dic = api(dn_prefix_www)
    except:
        print "fault detected in api response format, re-inserting...>>", dic
        # err_q.publish(dn)
        return
    try:
        dic['domain'] = dn
        to_insert_q.publish(json.dumps(dic))
        print "[i] DONE >>", dn
    except:
        print "[x] failed add to database, re-inserting...>>>>", dn
        # err_q.publish(dn)
        return
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return


if __name__ == "__main__":
    qn = 'error-domain-names'
    credentials = pika.PlainCredentials('guest', '***REMOVED***')

    def start_consume():
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue=qn)
        channel.basic_consume(callback, queue=qn, no_ack=False)
        channel.start_consuming()

    while True:
        time.sleep(2)                       # wait 2 seconds before reconnect
        print '[x] Starting a round of pika processing.'
        try:
            start_consume()
        except Exception as e:
            print "consuming Exception >>", e
