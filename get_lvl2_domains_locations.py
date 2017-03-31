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

def domain_to_ip(dn):
    a,b = get_ip_by_lvl2_domain(dn)
    if b == -1:
        return get_ip_by_prefix_www(dn),2
    else:
        return a,1


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




def callback(ch, method, properties, dn):
    ip, reftype = domain_to_ip(dn)
    if reftype == 1:
        api_res,status = ip_to_location(dn,ip)
        if status == 1:
            return
        if api_res[u'country_id'] != u'US':
            print 'still US...try Method 2'
    if reftype == 2:
        api_res,status = ip_to_location(dn,ip)
        if status == 1:
            return
        if api_res[u'country_id'] == u'US':
            print 'still US...try Method 2'


        _,b,c = tldextract.extract(i['DomainName'])
        second_lvl_dn = b+'.'+c
        second_lvl_dn_set.add(second_lvl_dn)





credentials = pika.PlainCredentials('guest', '***REMOVED***')
qn = 'second-lvl-domains'
def start_consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.4.249', 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue=qn)
    channel.basic_consume(callback, queue=qn, no_ack=False)
    channel.start_consuming()