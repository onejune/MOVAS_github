#coding=utf-8
import  pandas as pd
import numpy as np 
import os, sys
from pandas import DataFrame,Series
import re
import datetime

strategy = 'bucket'
imp_name = 'imp'
rev_name = 'rev'
ins_name = 'ins'
clk_name = 'clk'
req_name = 'req'
min_imp = 200
min_ins = 8

first = True

def groupby_process(df_total, key_list):
    global first
    starttime = datetime.datetime.now()
    p_str = ''
    df_total = df_total.groupby(key_list).agg({imp_name:['sum'], clk_name:['sum'], ins_name:['sum'], rev_name:['sum']})
    df_total['cpm'] = df_total[rev_name] * 1000 / df_total[imp_name]
    df_total['ivr'] = df_total[ins_name] / df_total[imp_name]
    
    df_total = df_total.reset_index()
    df_total = df_total[(df_total[(imp_name, 'sum')] >= min_imp) & (df_total[(ins_name, 'sum')] >= min_ins)]
    #print df_total.columns.values
    #print df_total.head()
    n = len(df_total.index.values.tolist())
    titles = [t[0] for t in df_total.columns.values]
    cnt_k = len(key_list)
    if first:
        p_str += 'key' + '\t' + '\t'.join(titles[cnt_k:]) + '\n'
        first = False
    for i, row in df_total.iterrows():
        key = [str(d) for d in row.values[0:cnt_k]]
        values = [str(d) for d in row.values[cnt_k:]]
        p_str += '#'.join(key) + '\t' + '\t'.join(values) + '\n'
    fout = open(output, 'a')
    fout.write(p_str)
    fout.close()
    endtime = datetime.datetime.now()
    time_use = (endtime - starttime).seconds
    print 'group key:', key_list, 'record_cnt:', n, 'time_use:', time_use

input_file = sys.argv[1]
output = sys.argv[2]

df = pd.read_csv(input_file, sep='\t')
print df.head()
print df.info()
print(df.columns)  #查看列名

groupby_process(df, ['unit_id', 'country_code', 'campaign_id', 'video'])
groupby_process(df, ['app_id', 'country_code', 'campaign_id', 'video'])
groupby_process(df, ['unit_id', 'country_code', 'packagename', 'video'])
groupby_process(df, ['app_id', 'country_code', 'packagename', 'video'])
groupby_process(df, ['country_code', 'campaign_id', 'video'])
groupby_process(df, ['country_code', 'packagename', 'video'])

groupby_process(df, ['unit_id', 'country_code', 'campaign_id', 'playable'])
groupby_process(df, ['app_id', 'country_code', 'campaign_id', 'playable'])
groupby_process(df, ['unit_id', 'country_code', 'packagename', 'playable'])
groupby_process(df, ['app_id', 'country_code', 'packagename', 'playable'])
groupby_process(df, ['country_code', 'campaign_id', 'playable'])
groupby_process(df, ['country_code', 'packagename', 'playable'])

groupby_process(df, ['unit_id', 'country_code', 'campaign_id', 'image'])
groupby_process(df, ['unit_id', 'country_code', 'packagename', 'image'])
groupby_process(df, ['app_id', 'country_code', 'campaign_id', 'image'])

