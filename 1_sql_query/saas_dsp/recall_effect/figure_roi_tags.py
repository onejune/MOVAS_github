import os, sys
import  pandas as pd
import numpy as np
import os, sys
from pandas import DataFrame,Series

fin = open('campaign_package_tag.txt')
cam_tags = {}
for line in fin:
    arr = line.strip().split('\t')
    cam_id = arr[1].lower()
    pro_tag = arr[6].lower()
    cam_tags[cam_id] = pro_tag
fin.close()

input_file = sys.argv[1]
output_file = sys.argv[2]
print('input_file:', input_file)
print('output_file:', output_file)

group_map = {}
fin = open(input_file)
for line in fin:
    arr = line.strip().split(',')
    req = '0'
    imp = '0'
    cost = '0'
    ins = '0'
    rev = '0'
    cr_ivr = '0'
    algo_price = '0'
    try:
        dtm,ad_type,platform,is_hb,ranker,package_name,traffic_package = arr[0].split('\t')
        req,imp,ins,rev,cost,cr_ivr,algo_price = arr[1:]
    except:
        continue
    #country_code = country_code.lower()
    try:
        req = float(req)
    except:
        req = 0
        pass
    try:
        imp = float(imp)
    except:
        imp = 0
        pass
    try:
        rev = float(rev)
    except:
        rev = 0
        pass
    try:
        ins = int(ins)
    except:
        ins = 0
        pass
    try:
        cost = float(cost)
    except:
        cost = 0
        pass
    try:
        cr_ivr = float(cr_ivr)
    except:
        cr_ivr = 0
        pass
    try:
        algo_price = float(algo_price)
    except:
        algo_price = 0
        pass

    #k = unit_id + '|' + country_code
    #cost_cpm = cost_map.get(k, 0)
    
    tags = cam_tags.get(package_name.lower(), 'none')
    traffic_tags = cam_tags.get(traffic_package.lower(), 'none')
    key = [dtm, ad_type, platform, is_hb, ranker, tags, traffic_tags]
    key_s = '\t'.join(key)
    group_map.setdefault(key_s, [0, 0, 0, 0, 0, 0, 0])
    group_map[key_s][0] += imp
    group_map[key_s][1] += ins
    group_map[key_s][2] += rev
    group_map[key_s][3] += cost
    group_map[key_s][4] += req
    group_map[key_s][5] += algo_price
    group_map[key_s][6] += cr_ivr

fout = open(output_file, 'w')
fout.write('dtm\tad_type\tplatform\tis_hb\tranker\tad_tags\ttraffic_tags\timp\tins\trev\tcost\treq\talgo_price\tcr_ivr\n')
for key in group_map:
    val = group_map[key]
    val_s = '\t'.join([str(d) for d in val])
    fout.write(key + '\t' + val_s + '\n')
fout.close()

