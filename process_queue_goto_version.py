# -*- coding: utf-8 -*-
import urllib
import urllib2
import ssl
import json
import rethinkdb as r
import tldextract
import socket
import pika
import time
from store_to_queue import Publisher
#from goto import with_goto



# ip -> 地区 阿里云API
host = 'https://dm-81.data.aliyun.com'
path = '/rest/160601/ip/getIpInfo.json'
appcode = '***REMOVED***'

###
# 从域名返回IP，比引入dns.resolver要快
# 0 - 三级域名即可返回IP
# 1 - 二级域名返回IP
# 2 - 二级域名前缀www.返回IP
# 3 - 上述方法均未返回IP
###
def get_ip_by_original_domain(dn):
    try:
        return socket.gethostbyname(dn),1
    except:
        return '',0

def get_ip_by_lvl2_domain(dn):
    try:
        ext = tldextract.extract(dn)
        return socket.gethostbyname(ext.domain + '.' + ext.suffix),2
    except:
        return '',0
    
def get_ip_by_prefix_www(dn):
    try:   
        return socket.gethostbyname('www.'+ext.domain + '.' + ext.suffix),4
    except:
        return '',0
         
    
def __ali_api(ip):
    '''
    阿里云API从IP返回地区（词典）
    '''
    print "Query IP >>>", ip
    ip = str(ip)
    querys = 'ip=' + ip
    url = host + path + '?' + querys
    request = urllib2.Request(url)
    request.add_header('Authorization', 'APPCODE ' + appcode)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    response = urllib2.urlopen(request, context=ctx)
    temp = response.read()
    return temp


def ip_to_location(dn,ip):
    '''
    参数:domainName,ip
    返回:pre_insert
    '''                                       
    api_response = __ali_api(ip)
    res = json.loads(api_response)
    status = res['code']
    data = res['data']
    if status == 1:                     # api return error code
        return data,1
    elif status == 0:
        data[u'DomainName'] = dn
        return data,0
    # try:                                            # 抓取ip查询地域API的报错
    #     api_response = __ali_api(ip)                                         # 抓取json格式转换错误
    #     res = json.loads(api_response)['data']
    #     res[u'DomainName'] = dn
    #     return res,0
    # except Exception as e:
    #     print "ALI API 异常 开始 ----------------"
    #     print e
    #     print "re inserting dn : ",dn
    #     print "ALI API 异常 结束 ----------------"
    #     return {},1

# COMMENT: has implicit context : err_q , conn
def put_to_db(record):
    try:
        primaryKey = record['DomainName']
        a = r.table('1_Domain_Location').get(primaryKey).replace(record).run(conn)
        return 0
    except Exception as e:
        print '[x] put_to_db Exception!!'
        print e
        return 1


def no_ip_found(dn):
    # pre_insert = {u'ip': u'', u'city': u'', u'area_id': u'', u'region_id': u'', u'area': u'', u'city_id': u'', u'country': u'', u'region': u'', u'isp': u'', u'country_id': u'', u'county': u'', u'isp_id': u'', u'county_id': u'', u'DomainName': dn}
    # pre_insert[u'RefDomainType'] = 3
    # put_to_db(pre_insert)
    # ch.basic_reject( delivery_tag = method.delivery_tag)
    return 1

def only_orig_ip(dn,orig_ip):
    pre_insert,status = ip_to_location(dn,orig_ip)
    if status == 1: return 1
    pre_insert[u'RefDomainType'] = 0
    put_to_db(pre_insert)
    return

def only_lvl2_ip(dn,lvl2_ip):
    pre_insert,status = ip_to_location(dn,lvl2_ip)
    if status == 1: return 1
    pre_insert[u'RefDomainType'] = 1
    put_to_db(pre_insert)
    return

def only_www_ip(dn,www_ip):
    pre_insert,status = ip_to_location(dn,www_ip)
    if status == 1: return 1
    pre_insert[u'RefDomainType'] = 2
    put_to_db(pre_insert)
    return

def use_lvl2_ip(dn,lvl2_ip, www_ip):
    pre_insert,status = ip_to_location(dn,lvl2_ip)
    if status == 1: return 1
    if pre_insert[u'country_id'] == u'US':
        only_www_ip(dn,www_ip)
        return
    pre_insert[u'RefDomainType'] = 1
    put_to_db(pre_insert)
    return

# 只会执行一种情况
def dispatcher(dn,orig_ip, lvl2_ip, www_ip, status_combine):
    return {
    0: no_ip_found(dn),                   # no ip found
    1: only_orig_ip(dn,orig_ip),           # only original
    2: only_lvl2_ip(dn,lvl2_ip),           # only lvl2 domain has ip
    3: only_lvl2_ip(dn,lvl2_ip),           # has both original ip and lvl2 domain ip
    4: only_www_ip(dn,www_ip),              # only prefix www has ip
    5: only_www_ip(dn,www_ip),              # has both original ip and prefix www ip
    6: use_lvl2_ip(dn,lvl2_ip, www_ip),    # has both lvl2 domain ip and prefix www ip
    7: use_lvl2_ip(dn,lvl2_ip, www_ip),    # has all of 3
}.get(status_combine,Exception)


def callback(ch, method, properties, dn):
    print " [x] Processing domain : %r" % dn

    # catched
    orig_ip, orig_status = get_ip_by_original_domain(dn)
    lvl2_ip, lvl2_status = get_ip_by_lvl2_domain(dn)
    www_ip, www_status = get_ip_by_prefix_www(dn)
    # catched

    status_combine = orig_status + lvl2_status + www_status
    # try:
    #     if dispatcher(orig_ip, lvl2_ip, www_ip, status_combine) != 1:
    #         ch.basic_ack( delivery_tag = method.delivery_tag)
    # except:
    #     print 'dispather ERROR!'
    if dispatcher(dn, orig_ip, lvl2_ip, www_ip, status_combine) != 1:
        ch.basic_ack( delivery_tag = method.delivery_tag)

if __name__ == "__main__":
    conn = r.connect(host='172.16.1.2', db='IP')
    credentials = pika.PlainCredentials('guest', '***REMOVED***')
    
    def start_consume():
        connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue='yilou-queue')
        channel.basic_consume(callback, queue='yilou-queue', no_ack=False)
        channel.start_consuming()

    while True:
        time.sleep(2)                       # wait 2 seconds before reconnect
        print '[x] Starting a round of pika processing.'
        try:
            start_consume()
        except Exception as e:
            print "consuming Exception >>",e

    # start_consume()
