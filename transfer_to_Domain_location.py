import rethinkdb as r

conn = r.connect(host='172.16.1.2',db='IP')

r.table_create('1_Domain_Location',primary_key='DomainName').run(conn)

