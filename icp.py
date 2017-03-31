import requests
import tldextract
import gevent
from gevent.pool import Pool
## init of request
session = requests.Session()
session.trust_env = False
## 

li = []
with open('short-domain-list.txt','r') as f:
    get_num = 10
    for i in f:
        if get_num == 0 : break
        li.append(i)
        get_num-=1

def async_icp(dn):
    ext = tldextract.extract(dn)
    temp = ext.domain + '.' + ext.suffix
    print "getting >>",temp
    r = session.get('http://www.icpchaxun.com/yuming/'+temp+'/')
    return r.text



concur_lim = 200
pool = Pool(200)
jobs = [gevent.spawn(async_icp,dn) for dn in li]

gevent.joinall(jobs, timeout = 3)
[job.value for job in jobs]


