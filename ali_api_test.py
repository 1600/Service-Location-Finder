#!coding=utf-8
import urllib2
import ssl

host = 'https://dm-81.data.aliyun.com'
path = '/rest/160601/ip/getIpInfo.json'
appcode = '***REMOVED***'


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
    temp = response.read()
    return temp

print ali_api('23.238.206.206')