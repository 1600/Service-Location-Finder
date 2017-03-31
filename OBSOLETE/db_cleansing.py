import rethinkdb as r
import dns.resolver

conn = r.connect(host='172.16.1.2',db='IP')

for i in r.table('Domain2Location').filter({'ip':''}).filter(lambda doc: doc['DomainName'].match(".gov.cn")).run(conn):
    print i
    break

print r.table("Domain2Location").get("0df627b7-f3ff-40af-a4cd-058485891c1e").run(conn)


myResolver = dns.resolver.Resolver()
ans = myResolver.query("www.huining.gov.cn","A")
for i in ans:
    print i