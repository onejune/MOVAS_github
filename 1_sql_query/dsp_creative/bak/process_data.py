import os, sys

dt_fin = {}

def add_data(key, value):
    dt_fin.setdefault(key, [0, 0, 0, 0])
    dt_fin[key][0] += value[0]
    dt_fin[key][1] += value[1]
    dt_fin[key][2] += value[2]
    dt_fin[key][3] += value[3]

input_path, output_path = sys.argv[1:]
fin = open(input_path)
for line in fin:
    key, imp, clk, ins, rev = line.strip().split('\t')
    try:
        exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id = key.split("#")
    except:
        continue
    try:
        imp = int(imp)
    except:
        imp = 0
    try:
        clk = int(clk)
    except:
        clk = 0
    try:
        ins = int(ins)
    except:
        ins = 0
    try:
        rev = float(rev)
    except:
        rev = 0
    ctr = clk * 1.0 / imp
    ivr = ins * 1.0 / imp
    cpm = rev * 1000 / imp
    k_lst = [exchanges,os,countrycode,adtype,appid,packagename,tempid,creative_id]
    crt_key1 = '#'.join(k_lst)
    k_lst = [exchanges, os, countrycode, adtype, packagename, tempid, creative_id]
    crt_key2 = '#'.join(k_lst)
    k_lst = [exchanges, os, adtype, packagename, creative_id]
    crt_key3 = '#'.join(k_lst)
    
    k_lst = [exchanges, os, adtype, packagename, tempid]
    tpl_key1 = '#'.join(k_lst)
    k_lst = [exchanges, os, adtype, appid, packagename, tempid]
    tpl_key2 = '#'.join(k_lst)
    k_lst = [exchanges, os, adtype, appid, tempid]
    tpl_key3 = '#'.join(k_lst)

    add_data(crt_key1, [imp, clk, ins, rev])
    add_data(crt_key2, [imp, clk, ins, rev])
    add_data(crt_key3, [imp, clk, ins, rev])

    add_data(tpl_key1, [imp, clk, ins, rev])
    add_data(tpl_key2, [imp, clk, ins, rev])
    add_data(tpl_key3, [imp, clk, ins, rev])

fout = open(output_path, 'w')
for k in dt_fin:
    imp, clk, ins, rev = dt_fin[k]
    if imp < 100 or ins < 3:
        continue
    ctr = clk * 1.0 / imp
    ivr = ins * 1.0 / imp
    cpm = rev * 1000 / imp
    s = "%s\t%d\t%d\t%d\t%f\t%f\t%f\t%f\n" % (k, imp, clk, ins, rev, ctr, ivr, cpm)
    fout.write(s)
