# -*- coding: utf-8 -*-
import httplib2
import urllib
import ssl
import json
import rethinkdb as r
import tldextract
import socket
import pika
import time
import Queue


# ip -> 地区 阿里云API
host = 'https://dm-81.data.aliyun.com'
path = '/rest/160601/ip/getIpInfo.json'
appcode = '***REMOVED***'
wait_reprocess_queue = Queue.Queue()

def domain_to_ip(dn):
    '''
    从域名返回IP，比引入dns.resolver要快
    RefDomainType
    0 - 三级域名即可返回IP
    1 - 二级域名返回IP
    2 - 二级域名前缀www.返回IP
    3 - 上述方法均未返回IP
    '''
    try:
        return socket.gethostbyname(dn),0
    except:
        try:
            ext = tldextract.extract(dn)
            return socket.gethostbyname(ext.domain + '.' + ext.suffix),1
        except:
            try:
                return socket.gethostbyname('www.'+ext.domain + '.' + ext.suffix),2       
            except:
                return '',3
    
    
def ali_api(ip):
    '''
    阿里云API从IP返回地区（词典）
    '''
    print("Converting ip >>>", ip)
    ip = str(ip)
    querys = 'ip=' + ip
    url = host + path + '?' + querys
    request = urllib.Request(url)
    request.add_header('Authorization', 'APPCODE ' + appcode)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    response = urllib.request.urlopen(request, context=ctx)
    temp = response.read()
    return temp


def ip_to_location(dn,ip,ch):
    '''
    参数:ip, channel
    返回:pre_insert
    '''
    try:                                            # 抓取ip查询地域API的报错
        api_response = ali_api(ip)                                         # 抓取json格式转换错误
        res = json.loads(api_response)['data']
        res[u'DomainName'] = dn
        return res,0
    except Exception as e:
        print("ALI API 异常 开始 ----------------")
        print(e)
        print("re inserting dn : ",dn)
        print("ALI API 异常 结束 ----------------")
        return {},1


def callback(ch, method, properties, dn):
    print(" [x] Processing domain : %r" % dn)
    pre_insert = {}
    temp_ip, category = domain_to_ip(dn)

    if category == 0:                                   # 0 - 三级域名即可返回IP
        pre_insert,status = ip_to_location(dn,temp_ip,ch)
        if status == 1: return
        pre_insert[u'RefDomainType'] = 0
        print('[i] Original   DN -> IP -> Location',pre_insert)
    elif category == 1:                                 # 1 - 二级域名返回IP
        pre_insert,status = ip_to_location(dn,temp_ip,ch)
        if status == 1: return
        pre_insert[u'RefDomainType'] = 1
        print('[i] Second Lv. DN -> IP -> Location',pre_insert)
    elif category == 2:                                 # 2 - 二级域名前缀www.返回IP
        pre_insert,status = ip_to_location(dn,temp_ip,ch)
        if status == 1: return
        pre_insert[u'RefDomainType'] = 2
        print('[i] Prefix www.DN -> IP -> Location',pre_insert)
    elif category == 3:                               # 3 - 上述方法均未返回IP
        pre_insert = {u'ip': u'', u'city': u'', u'area_id': u'', u'region_id': u'', u'area': u'', u'city_id': u'', u'country': u'', u'region': u'', u'isp': u'', u'country_id': u'CN', u'county': u'', u'isp_id': u'', u'county_id': u'', u'DomainName': dn}
        pre_insert[u'RefDomainType'] = 3
        print('[i] ..............No IP............',pre_insert)
    else:
        with open('ERRORLOG_dn2loc.txt') as f:
            f.write('improbable error on %r' % dn)
        return
    r.table('Domain_Location').insert(pre_insert).run(conn)
    ch.basic_ack( delivery_tag = method.delivery_tag)
    return

    
if __name__ == "__main__":
    conn = r.connect(host='172.16.1.2', db='IP')
    credentials = pika.PlainCredentials('guest', '***REMOVED***')
    
    def start_consume():
        connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue='domain-queue')
        channel.basic_consume(callback, queue='domain-queue', no_ack=False)
        channel.start_consuming()

    while True:
        time.sleep(2)                       # wait 2 seconds before reconnect
        print('[x] Starting a round of pika processing.')
        try:
            start_consume()
        except Exception as e:
            print("consuming Exception >>",e)


