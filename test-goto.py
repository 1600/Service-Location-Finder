# def f(a):
#     def g(b, c, d, e):
#         print(a, b, c, d, e)
#     return g
 
# f1 = f(1)
# print f1(2,3,4,5)


def no_ip_found(ip):
    pass
def only_orig_ip(ip):
    pass
def use_lvl2_ip(ip):
    pass
def use_pref_ip(dn):
    print dn
    pass

def orig_ip_not_US(ip):
    pass

def dispatcher(ip,status_combine):
    return {
    0: no_ip_found(ip),
    1: only_orig_ip(ip),
    2: use_lvl2_ip(ip),
    3: use_lvl2_ip(ip),
    4: use_pref_ip(ip),
    5: use_lvl2_ip(ip),
    6: use_lvl2_ip(ip),
    7: use_lvl2_ip(ip),
}.get(status_combine,Exception)



print dispatcher('bitch',4)