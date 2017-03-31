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
    'Cookie': 'UM_distinctid=15ac5f8d2a03-005ffdaad24be2-5b123112-100200-15ac5f8d2a13f1; channel=W4x0IkTg1jQi1lpY0MQMjg%3D%3D; aliyun_choice=CN; login_aliyunid="***REMOVED***"; login_aliyunid_ticket=mV*s*CT58JlM_1t$w3eB$e*FK5yWzNSlN67azXM0Q7Hflx8DC2rqSXdu*CX8_M_pof_BNTwUhTOoNC1ZBeeMfKJzxdnb95hYssNIZor6q7SCxRtgmGCbifG2Cd4ZWazmBdHI6sgXZqg4XFWQfyKpeuv00; login_aliyunid_csrf=_csrf_tk_1509590769911535; ***REMOVED***; hssid=1m5L0V4X4jFwTsK6Sm3Z_OQ1; hsite=6; aliyun_country=CN; aliyun_site=CN; industry-tip=true; _HOME_JSESSIONID=E1666S91-BPWI773TM4YC8N8QTEEL1-KOBXMU0J-RPGB; _home_session0=WoSyfIOIszdSbR5eNII89XwjPGH%2BfOsaXlzxNPAkFYyi4DkPnxhfwX%2FUScuu3tP1nmz3TcKHbDNGfkzsI2oU6jrodRNWNzNIRNideULC7izi1xGda%2FbHEWjegh%2BhIrtGCNYHLFL1c2t8Pv%2B0esdPXFukLwqo%2FiWRjxKu4QmTL88ErJBwGtqoNxdcYrPyVkpI4EYnOaNqIWK8s4RDM4oX%2Bf4OooD1Im8PSVUu%2BsSNdmLXY%2FOJN5hjobyKazjLjzs5Adsm385%2FzwpGM2X3pTImHXcHhO0uTm1oQp74HshP1eLRVPXmrdBcEl5h%2FJeSQjrTVjjZT8bP1M81CRCUfjYi7Q%3D%3D; consoleNavVersion=1.4.48; SLBNEW_JSESSIONID=8N566391-P1IJUUZDN3GUG0M5VMDU2-4M12NU0J-EL4; _slbnew_session0=qEN%2B4%2F9FDRR46%2FNmKAZORajtegzs0X7ZHV3mYUEC6%2BYpdJ7%2FAk1Xt2DIAtN3JDrglv5hk1ZEY9DDYv5caig2aSUJ2YyqHoxmMfpl0%2FnXO5b7hK3s5FDnzlyua5TEoTpZNAAY9qDaUIHvWCmGPlnyuvFR42mGScdEEsgkxKsyuh0AE6y%2BeeMZnz%2F9rHC0b%2FGzB6RS8tjIt3RVJLCPKkJTPcKlO1BbP1kw82DIHC8ugNX1vG%2B3VRPh8qDBmqzJXzZaFufKqZ39fGWGAkUdzapPmtB4t9B9tl3RU69Wj8OEc8uuuGyZmdcjd4SIWVmFkkebkZnokEPxQHHNuzgu7RWFjQ%3D%3D; COMMONBUY_JSESSIONID=T1666A81-X3BJR9Y9NI46A31FSBZB2-BG62NU0J-ACB; _commonbuy_session0=qEN%2B4%2F9FDRR46%2FNmKAZORZgKmbUfY0IsvLGivYWGYe9%2F2bZF0ufEe6SyOal8U2%2BO2itwnYwI9JZ7lHip%2BJGmTWQxT7Ou075WnUX0YYuoa%2BRJGimo9F%2F01z5FIHg8ykRiapK9v1tKtZE%2Flo20mmDpEMptcq1cMqEw57RU%2FF3KCHhYtvnxSn51CEGlHq04JhI1QAf0h75IDHFyX2lgqFbKevD3wHp%2FR3nhTIbRikdYdnG5OGolnZpPY5A0LNehgQ%2BHMYPy2btp58uhvWGoZvIsuYCX7pXZlq8uEuJokPICQPzU6r3zs6ni%2FJaCxlwEXASvPTmaAEeI4oXil344%2BbsOKQ%3D%3D; cna=29Y6EYeZFmkCAbeB2umWKlbE; _ga=GA1.2.1400381807.1471927483; JSESSIONID=0N566JD1-X46IJWPKUAEKA3QB1KD62-M9LYNU0J-2CD; console0=BPBdD07Tvdk1xD9U4NnzYaw7Fdln%2B6gsRgmb92BlfnwM2SE9pCy0klRPVgXyIO2PeikAWfr2Rh9oBIjFd6RhLdqio2CSVfxlWjvrdEbVXxbpCsZao%2B8adp%2BF51sfX%2BAiam5U8EoRcj30EPkG%2F%2FzAH5%2Bon%2FAIqKbINAdPCuaLemH%2FvIp5QgmY0d8seQlLKrdhgch8wDG6bEmnFwPOuHmWeeY5B64qhCzGf6ikktRnbrCmVMRqi6%2BJsMNqUXdHEQvk61fRlYnfQWGOFUgoh2pmbJZcOuHosPVkPCrsx%2BM45cCZM%2BYlWVyaicusHdnP9OtsixdX63JUB8Tlfj0rz7EwLQ%3D%3D; consoleRecentVisit=dns%2Cslb; l=Ai8v8Mv51umT0uSskZpjFfBaP0g7d4P2; isg=AqGhnUlfXavK6P429Xx3rL-1sG2OUDzrLPQZIgN0PqgHasI8W5-kEeLsuiiX',
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


def process_dn(dn):
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
        to_insert_q.publish(json.dumps(dic))
        print "[i] DONE >>", dn
    except:
        print "[x] failed add to database, re-inserting...>>>>", dn
        # err_q.publish(dn)
        return
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return


def run():
    dn_queue = Queue.Queue()
    pool_size = 50
    pool = Pool(pool_size)
    with open('domain-list.txt','r') as f:
        for i in f:
            dn_queue.put(i)

    while dn_queue.qsize() > 0:
        pool.spawn(process_dn,dn_queue.get())
    pool.join()


run()
