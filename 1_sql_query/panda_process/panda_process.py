#coding=utf-8
import  pandas as pd
import numpy as np 
import os, sys
from pandas import DataFrame,Series
import re

strategy = 'bucket'
imp_name = 'total_imp'
rev_name = 'total_rev'

input_file = sys.argv[1]

df = pd.read_csv(input_file, sep='\t')
print df.head()

print(df.columns)  #查看列名
print(df.dtypes)    #查看各列数据类型
#print(df.head(20)) #查看前20行数据
#df=df.loc[:,'dtm':strategy]     #选择FFMC到rain列所有数据

print(df[strategy].unique())     #输出month列唯一值
#print(df['country_code'].value_counts())   #输出month列各变量出现频数

df_group = df[imp_name].groupby([df['country_code'],df[strategy]]) #根据month和day列对DC列进行聚合
df_fun = df_group.agg(['sum','mean','std']) #对df_group求和，均值和标准差
print(df_fun.head())

df_toushi = pd.pivot_table(df,index=['country_code'],columns=[strategy],
        values=[imp_name],aggfunc=[np.sum,np.mean],fill_value=0)
print(df_toushi.head())

df_paixu = df.sort_values(by=[imp_name, rev_name], ascending=[0,1]) 

print '\n-------过滤cn us:'
df_cn = df[df['country_code'].isin(['cn','us'])] 
df_cpm = df_cn.groupby(strategy).agg({imp_name:['sum'], rev_name:['sum']})
df_cpm['cpm'] = df_cpm[rev_name] * 1000 / df_cpm[imp_name]
print df_cpm.head()

print '*' * 10 + 'top country' + '*' * 10
df_rev = df.groupby(['country_code', strategy]).agg({rev_name:['sum'], imp_name:['sum']})
df_rev['cpm'] = df_rev[rev_name] * 1000 / df_cpm[imp_name]

total_imp = df[imp_name].sum()
total_rev = df[rev_name].sum()
print 'total_imp=', total_imp, 'total_rev=', total_rev
#print df_rev.index.names
#print df_rev.index.values

#print df_rev.columns.names
#print df_rev.columns.values

#print df_rev.values
#print df_rev[(imp_name, 'sum')]
#print df_rev[(rev_name, 'sum')]

print '\n-------遍历multi-index:'
n = len(df_rev.index.values.tolist())
print n
for i in range(10):
    print i, df_rev.index.values[i], df_rev.values[i]

print '\n--------每天的roi:'
df_day = df.groupby(['dtm']).agg({rev_name:['sum'], imp_name:['sum'], 'total_cost':['sum']})
df_day['roi'] = df_day[rev_name] / df_day['total_cost']
print df_day

print '\n--------每个实验roi:'
df_sty = df.groupby(['dtm', strategy]).agg({rev_name:['sum'], imp_name:['sum'], 'total_cost':['sum']})
df_sty['roi'] = df_sty[rev_name] / df_sty['total_cost']
print df_sty
df_sty.reset_index().to_csv('output/new_table.csv') 

print '\n--------top10 rev country:'
df_cc = df.groupby(['country_code']).agg({rev_name:['sum'], imp_name:['sum'], 'total_cost':['sum']})
df_cc['roi'] = df_cc[rev_name] / df_cc['total_cost']
#print df_cc.head()
sort_df = df_cc.reset_index().sort_values((rev_name, 'sum'),ascending = False).set_index('country_code')
print sort_df.head(10)

print '\n--------实验分析(保存到excel):'
df_sty = df.groupby(['dtm', strategy, 'ad_type']).agg({'req':['sum'], rev_name:['sum'], imp_name:['sum'], 'total_cost':['sum']})
print df_sty.head()
df_sty.reset_index().to_csv('output/strategy_effect.csv') 

print '\n--------iterrows遍历df:'
df_n = df.groupby(['country_code', 'unit_id', strategy]).agg({rev_name:['sum'], imp_name:['sum'], 'total_cost':['sum']})
#df_n = df_n.reset_index()
for row_index, row in df_n.head().iterrows():
    print row_index, row.values
for row in df_n.head().itertuples():
    print row, getattr(row, 'Index')


