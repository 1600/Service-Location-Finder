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
from sets import Set


# ip -> 地区
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
        return socket.gethostbyname(dn),0
    except:
        return '',-1


def get_ip_by_lvl2_domain(dn):
    try:
        ext = tldextract.extract(dn)
        temp = ext.domain + '.' + ext.suffix
        return socket.gethostbyname(temp),1
    except:
        return '',-1
    
def get_ip_by_prefix_www(dn):
    try:
        return socket.gethostbyname('www.'+ext.domain + '.' + ext.suffix),2
    except:
        return '',-1

# def domain_to_ip(dn):
#     try:
#         return get_ip_by_original_domain(dn)
#     except:
#         try:
#             return get_ip_by_lvl2_domain(dn)
#         except:
#             try:
#                 return get_ip_by_prefix_www(dn)
#             except:
#                 return '',3

def try_get_cn_ip(dn):
    pass


# TO DOC
#
def ali_api(ip):
    '''
    Private Method, cannot invoke outside class
    阿里云API从IP返回地区（词典）
    '''
    ip = str(ip)
    querys = 'ip=' + ip
    url = host + path + '?' + querys
    request = urllib2.Request(url)
    request.add_header('Authorization', 'APPCODE ' + appcode)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    response = urllib2.urlopen(request, context=ctx)
    return response.read()



def ip_to_location(dn,ip):
    '''
    参数:dn,ip
    返回:pre_insert
    '''
    try:                                            # 抓取ip查询地域API的报错 和抓取json格式转换错误
        print "[i] <ip_to_location> Converting ip >>>", ip
        api_response = ali_api(ip)
        res = json.loads(api_response)
        status = res['code']
        if status == 1:
            print '[i] <ip_to_location> ALIAPI return ERROR CODE 1 >>',res['data']
            return {},1 
        res['data'][u'DomainName'] = dn
        return res['data'],0
    except Exception as e:
        print "[x] <ip_to_location>",e
        print "[x] <ip_to_location> re inserting dn : ",dn
        return {},1


def callback(ch, method, properties, record):
    time.sleep(0.1)
    '''处理所有美国IP的记录,即'country'=u'美国'
    '''
    record = json.loads(record)
    rid = record['id']
    dn = record['DomainName']
    print "Processing======================"
    print record
    #record['RefDomainType']

    try_ip,_reftype = get_ip_by_lvl2_domain(dn)                 
    if _reftype != -1:                                                # 如果2级域名有IP
        print 1111
        api_res,status = ip_to_location(dn,try_ip)
        print '1111_Status',status
        if status == 1: 
            print "API failing returnning END==========="
            return
        print 'status >>', status
        print 'api_res >>', api_res,type(api_res)
        if api_res[u'country_id'] == u'US':
            print 'lvl2 Domain ip_to_Location still US...try Prefix WWW'
            try_ip,_reftype = get_ip_by_prefix_www(dn)
            if _reftype != -1:                                       # 如果prefix www 有IP
                print 2222
                api_res,status = ip_to_location(dn,try_ip)
                print '22222_status',status
                if status == 1: 
                    print 'Prefix WWW ip_to_location failed,,,returned'
                    print "Processing END======================"
                    ch.basic_ack( delivery_tag = method.delivery_tag)
                    return
                if api_res['country_id'] == u'US':
                    print 'Prefix WWW still US in ...stopping'
                    print "Processing END======================"
                    ch.basic_ack( delivery_tag = method.delivery_tag)
                    return
                else:
                    print 'found non US ip...',api_res['country_id']
                    print api_res
                    api_res['RefDomainType'] = 2
                    api_res['id']=rid
                    #raw_input()
                    r.table('Domain_Location').get(rid).replace(api_res).run(conn)
                    ch.basic_ack( delivery_tag = method.delivery_tag)
                    print "Processing END======================"
                    return
            else:
                print 'get_ip_by_prefix_www failed...returned'
                print "Processing END======================"
                ch.basic_ack( delivery_tag = method.delivery_tag)
                return
        else:
            print 'found non US ip...',api_res['country_id']
            print api_res
            api_res['RefDomainType'] = 1
            api_res['id']=rid
            #raw_input()
            r.table('Domain_Location').get(rid).replace(api_res).run(conn)
            print "Processing END======================"
            ch.basic_ack( delivery_tag = method.delivery_tag)
            return
    else:
        print 'lvl2 dn to ip failed, try prefix www...'
        try_ip,_reftype = get_ip_by_prefix_www(dn)
        if _reftype != -1:                                              # 2级域名没IP，检测prefix www是否有IP
            print 3333
            api_res,status = ip_to_location(dn,try_ip)
            if status == 1: 
                print "API failing returnning END==========="
                return
            if api_res['country_id'] == u'US':
                print 'still US in Method 3...stopping'
                print "Processing END======================"
                ch.basic_ack( delivery_tag = method.delivery_tag)
                return
            else:
                print 'found non US ip...',api_res['country_id']
                print 'replacing with >>',api_res
                api_res['RefDomainType'] = 2
                api_res['id']=rid
                #raw_input()
                r.table('Domain_Location').get(rid).replace(api_res).run(conn)
                print "Processing END======================"
                ch.basic_ack( delivery_tag = method.delivery_tag)
                return
        else:
            print 'get_ip_by_lvl2_domain failed...returned'
            print "Processing END======================"
            ch.basic_ack( delivery_tag = method.delivery_tag)
            return

    
    print "Processing END======================"
    ch.basic_ack( delivery_tag = method.delivery_tag)
    return

    
if __name__ == "__main__":
    conn = r.connect(host='172.16.1.2', db='IP')
    credentials = pika.PlainCredentials('guest', '***REMOVED***')
    lvl2_domain_set = Set()

    qn = 'fake-US-ip'
    
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

    # while True:
    #     time.sleep(2)                       # wait 2 seconds before reconnect
    #     print '[x] Starting a round of pika processing.'
    #     start_consume()

