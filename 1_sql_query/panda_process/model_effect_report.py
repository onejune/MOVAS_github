#coding=utf-8
import  pandas as pd
import numpy as np 
import os, sys
from pandas import DataFrame,Series
import re

strategy = 'bucket'
imp_name = 'total_imp'
rev_name = 'total_rev'
ins_name = 'install'
clk_name = 'click'
cost_name = 'total_cost'
req_name = 'req'

def groupby_process(df_total, output):
    p_str = ''
    df_total = df_total.groupby([strategy]).agg({req_name:['sum'], imp_name:['sum'], clk_name:['sum'], ins_name:['sum'], rev_name:['sum'], cost_name:['sum']})
    df_total['cpm'] = df_total[rev_name] * 1000 / df_total[imp_name]
    df_total['roi'] = df_total[rev_name] / df_total[cost_name]
    df_total['fillrate'] = df_total[imp_name] / df_total[req_name]
    df_total['acp'] = df_total[rev_name] / df_total[imp_name]
    df_total['ctr'] = df_total[clk_name] / df_total[imp_name]
    df_total['cvr'] = df_total[ins_name] / df_total[clk_name]
    index_name = df_total.index.names[0]
    #n = len(df_total.index.values.tolist())
    titles = [t[0] for t in df_total.columns.values]
    p_str += index_name + '\t' + '\t'.join(titles) + '\n'
    for row_index, row in df_total.iterrows():
        name = row_index
        values = [str(u) for u in row.values]
        p_str += name + '\t' + '\t'.join(values) + '\n'
    fout = open(output, 'w')
    fout.write(p_str)
    fout.close()

input_file = sys.argv[1]
if len(sys.argv) <= 2:
    flag = 'dat'
else:
    flag = sys.argv[2]

df = pd.read_csv(input_file, sep='\t')
print df.head()
print df.info()
print(df.columns)  #查看列名

#total effect 
groupby_process(df, 'temp/total' + '.' + flag)

#us effect
df_us = df[df['country_code'].isin(['us'])] 
groupby_process(df_us, 'temp/us' + '.' + flag)

#cn effect
df_cn = df[df['country_code'].isin(['cn'])] 
groupby_process(df_cn, 'temp/cn' + '.' + flag)

#rviv effect
df_rviv = df[df['ad_type'].isin(['rewarded_video', 'interstitial_video', 'more_offer'])] 
groupby_process(df_rviv, 'temp/rviv' + '.' + flag)

#hb effect
df_rviv = df[df['is_hb'].isin([1])] 
groupby_process(df_rviv, 'temp/hb' + '.' + flag)


