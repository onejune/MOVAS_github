#########################################################################
# File Name: compare_replay_ivr.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 05 Nov 2020 02:23:19 PM CST
#########################################################################
#!/bin/bash
import os, sys

base_data=sys.argv[1]
exp_data=sys.argv[2]

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

base_ivr = get_ivr(base_data)
exp_ivr = get_ivr(exp_data)

for extra5 in base_ivr:
    base_v = base_ivr[extra5]
    exp_v = exp_ivr.get(extra5, 'null')
    print extra5 + '\t' + base_v + '\t' + exp_v
