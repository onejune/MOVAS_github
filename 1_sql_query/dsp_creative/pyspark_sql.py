# -*- coding: utf-8 -*-
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
spark = SparkSession\
        .builder\
        .appName("adx_srs_creative")\
        .enableHiveSupport() \
        .getOrCreate()
begin_date, end_date, output = sys.argv[1:]

full_key = "exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id"

sql_imp1="""
select ext2 as ctype,exchanges,os,countrycode,get_json_object(ext10,'$.reqtype') as adtype,appid,cpackagename as packagename,campaignid,split(get_json_object(ext3,'$.creative'),'#')[3] as tempid,split(get_json_object(ext3,'$.creative'),'#')[2] as creative_list
from adn_dsp.log_adn_dsp_impression_hour
where concat(yr,mt,dt,hh)>='%s' and concat(yr,mt,dt,hh)<='%s'
and price<100
""" % (begin_date, end_date)

sql_imp2="""
select %s,count(1) as imp
from(
    select exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id,ctype
    from tmp_imp_table
    lateral view explode(split(creativeids,',')) t as creative_id
)
group by %s
""" % (full_key, full_key)

sql_click1="""
select ad_type as exchanges,platform as os,country_code as countrycode, get_json_object(template,'$.reqtype') as adtype,strategy as appid,
ext_campaignpackagename as packagename,campaign_id as campaignid,get_json_object(get_json_object(get_json_object(ext_stats,'$.value'),'$.creative'),'$.ad_template_id') as tempid,
get_json_object(get_json_object(get_json_object(ext_stats,'$.value'),'$.creative'),'$.group_list') as creative_list
from dwh.ods_adn_trackingnew_click_cpc
where algorithm in ('normal','justcons','justbid','dsp_normal','dsp_just_consume') and publisher_id='6028' 
and concat(yyyy,mm,dd,hh)>='%s' and concat(yyyy,mm,dd,hh)<='%s'
"""  % (begin_date, end_date)

sql_click2="""
select %s,count(1) as clk
from(
    select exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id
    from tmp_clk_table
    lateral view explode(split(creativeids,',')) t as creative_id
)
group by %s
""" % (full_key, full_key)

sql_ins1="""
select ad_type as exchanges,platform as os,country_code as countrycode, get_json_object(template,'$.reqtype') as adtype,strategy as appid,
ext_campaignpackagename as packagename,campaign_id as campaignid,get_json_object(get_json_object(get_json_object(ext_stats,'$.value'),'$.creative'),'$.ad_template_id') as tempid,
get_json_object(get_json_object(get_json_object(ext_stats,'$.value'),'$.creative'),'$.group_list') as creative_list,
if(split(ext_bp,'\"')[0] is NULL,0.0,split(ext_bp,'\"')[1]) as price
from dwh.ods_adn_trackingnew_install
where algorithm in ('normal','justcons','justbid','dsp_normal','dsp_just_consume') and publisher_id='6028' 
and concat(yyyy,mm,dd,hh)>='%s' and concat(yyyy,mm,dd,hh)<='%s'
"""  % (begin_date, end_date)

sql_ins2="""
select %s,count(1) as ins,sum(price) as rev
from(
    select exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id,price
    from tmp_ins_table
    lateral view explode(split(creativeids,',')) t as creative_id
)
group by %s
""" % (full_key, full_key)

def build_query_sql(key, imp_lower, ins_lower):
    sql_k1 = """
    select concat_ws('#', %s) as key, sum(imp) as imp, sum(clk) as clk, sum(ins) as ins, sum(rev) as rev,
        sum(clk) / sum(imp) as ctr, sum(ins) / sum(imp) as ivr, sum(rev) * 1000 / sum(imp) as cpm
    from joinRes_tmp_table
    group by concat_ws('#', %s)
    having imp>=%s and ins>=%s
    """ % (key, key, imp_lower, ins_lower)
    return sql_k1

@udf
def get_imp_creativeids(creative_list):
    creative_ids = []
    print(type(creative_list))
    print('creative_list=%s' % creative_list)
    try:
        for item in creative_list.split("|"):
            cur_creative = item.split(",")
            creativeid, adv_creative_id, creative_type = cur_creative[0:3]
            if creative_type in['201', '106', '61002']:
                creative_ids.append(creative_type + '_' + creativeid)
    except: 
        print('exception: creative_list%s=' % creative_list)
    return ','.join(creative_ids)

@udf
def get_click_creativeids(creative_list):
    creative_ids = []
    try:
        creative_list = eval(creative_list)
        for creative in creative_list:
            creativeid = creative.get('adn_creative_id', '')
            creative_type = creative.get('creative_type')
            if creativeid != '' and creative_type in['201', '106', '61002']:
                creative_ids.append(creative_type + '_' + creativeid)
    except:
        print('exception: creative_list%s=' % creative_list)
    return ','.join(creative_ids)

#先基于最全的维度把imp/clk/ins join到一个表里
# imp临时表: creativeids
imp_data = spark.sql(sql_imp1)
imp_data = imp_data.withColumn('creativeids', get_imp_creativeids('creative_list'))
imp_data.createOrReplaceTempView("tmp_imp_table")
# imp聚合表
imp_data2 = spark.sql(sql_imp2)
# 保存到 s3
#imp_data2.repartition(10).write.mode('overwrite').csv(output_prefix + '/imp', sep='\t')

# click临时表: creativeids
click_data = spark.sql(sql_click1)
click_data = click_data.withColumn('creativeids', get_click_creativeids('creative_list'))
click_data.createOrReplaceTempView("tmp_clk_table")
# click聚合表
click_data2 = spark.sql(sql_click2)
# 保存到 s3
#click_data2.repartition(10).write.mode('overwrite').csv(output_prefix + '/click', sep='\t')

#install 临时表: creativeids
ins_data = spark.sql(sql_ins1)
ins_data = ins_data.withColumn('creativeids', get_click_creativeids('creative_list'))
ins_data.createOrReplaceTempView("tmp_ins_table")
# install 聚合表
ins_data2 = spark.sql(sql_ins2)
# 保存到 s3
#ins_data2.repartition(10).write.mode('overwrite').csv(output_prefix + '/install', sep='\t')

#join imp/clk/ins
joinType = "left_outer"
joinKey = ['exchanges','os','countrycode','adtype','appid','packagename','campaignid','tempid','creative_id']
joinRes = imp_data2.join(click_data2, joinKey, joinType)
joinRes = joinRes.join(ins_data2, joinKey, joinType)

joinRes.createOrReplaceTempView("joinRes_tmp_table")

#然后基于素材和模板的回退维度计算各个指标
#build ivr/cpm
k_1 = "exchanges,os,countrycode,adtype,appid,packagename,tempid,creative_id"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = spark.sql(crt_k1_sql)

k_1 = "exchanges, os, countrycode, adtype, packagename, tempid, creative_id"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "os, countrycode, adtype, packagename, tempid, creative_id"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "exchanges, os, adtype, packagename, creative_id"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "exchanges, os, adtype, appid, packagename, tempid"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "exchanges, os, adtype, packagename, tempid"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "os, adtype, packagename, tempid"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

k_1 = "exchanges, os, adtype, appid, tempid"
crt_k1_sql = build_query_sql(k_1, 0, 0)
df_ = df_.union(spark.sql(crt_k1_sql))

df_.repartition(10).write.option("header", "true").mode('overwrite').csv(output, sep='\t')






