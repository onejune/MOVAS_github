#########################################################################
# File Name: compare_replay_ivr.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 05 Nov 2020 02:23:19 PM CST
#########################################################################
#!/bin/bash
import os, sys

online_data=sys.argv[1]
offline_data=sys.argv[2]

def get_ivr(data_dir):
	res = {}
	fin = open(data_dir)
	for line in fin:
		arr = line.strip().split('\t')
		cnt = len(arr)
		extra5 = arr[2].split('|')[0]
                ad_type = arr[19]
		ivr = arr[cnt - 1]
                ivr = ivr.strip('[]')
		res[extra5 + '\t' + ad_type] = ivr
		#print extra5, ivr
	fin.close()
	return res

offline_ivr = get_ivr(offline_data)
online_ivr = {}

for line in open(online_data):
    arr = line.strip().split('\t')
    req_id = arr[1]
    adtype = arr[3]
    unit_id = arr[5]
    ivr = arr[7]
    online_ivr[req_id + '\t' + adtype] = ivr

for extra5 in online_ivr:
    online_v = online_ivr[extra5]
    offline_v = offline_ivr.get(extra5)
    if not offline_v:
        continue
    print extra5 + '\t' + online_v + '\t' + offline_v
