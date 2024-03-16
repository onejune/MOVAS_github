import os, sys

base_data=sys.argv[1]
exp_data=sys.argv[2]
online_data=sys.argv[3]

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

online_ivr = {}

for line in open(online_data):
    arr = line.strip().split('\t')
    req_id = arr[1]
    adtype = arr[3]
    unit_id = arr[5]
    ivr = arr[6]
    online_ivr[req_id + '\t' + adtype] = ivr

for key in online_ivr:
	online_v = online_ivr[key]
	base_v = base_ivr.get(key)
	exp_v = exp_ivr.get(key)
        if not base_v or not exp_v:
            continue
	print key + '\t' + base_v + '\t' + exp_v + '\t' + online_v
