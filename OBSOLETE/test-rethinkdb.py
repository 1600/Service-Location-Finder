import rethinkdb as r
import json

conn = r.connect(host='172.16.1.2', db='TestDB')

tb = r.table("testable")


for i in xrange(10):
    tb.insert({'id':i,'data':2}).run(conn)

#--
#tb.get(3).replace({'id':55}).run(conn)
print tb.get(3).run(conn)
print tb.get(3).replace({'data':3}).run(conn)

# # c = 1
# temp = []
# for i in r.db('IP').table('Domain_Location').filter({}).run(conn):
#     if c == 0: break
#     print i
#     c-=1
#     print type(i)
#     temp.append(i)

# mod_rec = temp[0]
# print mod_rec.pop('id')
# print mod_rec