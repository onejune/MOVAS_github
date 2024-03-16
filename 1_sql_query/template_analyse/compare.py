#import os, sys

hive_dict = {}
fin = open('hive_data.dat')
for line in fin:
    arr = line.strip().split(',')
    key = arr[0]
    tpl = arr[1]
    try:
        imp = int(arr[2])
        ins = int(arr[3])
    except:
        continue
    hive_dict.setdefault(key, [])
    hive_dict[key].append([tpl, imp, ins, ins * 1.0 /imp])
fin.close()

kudo_dict = {}
imp_dict = {}
fin = open('tracking_data.dat')
for line in fin:
    arr = line.strip().split(',')
    tpl = arr[0]
    platform = arr[1]
    key = arr[2]
    try:
        ins = int(arr[5])
        imp = int(arr[4])
        if imp <= 500 or ins <= 10:
            continue
    except:
        continue
    kudo_dict.setdefault(key, [])
    kudo_dict[key].append([tpl, imp, ins, ins * 1.0 /imp])
    imp_dict.setdefault(key, 0)
    imp_dict[key] += imp
fin.close()

sorted_data = sorted(imp_dict.iteritems(),key=lambda d:d[1],reverse=True)
for ele in sorted_data:
    key = ele[0]
    imp = ele[1]
    k_d = kudo_dict[key]
    k_d.sort(lambda x,y:cmp(x[3], y[3]), reverse = True) 
    h_d = hive_dict.get(key)
    if h_d:
        h_d.sort(lambda x,y:cmp(x[3], y[3]), reverse = True)
    print imp, key, '\n', k_d, '\n', h_d
    

