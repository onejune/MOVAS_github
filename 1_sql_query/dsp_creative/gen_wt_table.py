# -*- coding: utf-8 -*-
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql import functions as F

spark = SparkSession\
        .builder\
        .appName("adx_srs_creative")\
        .enableHiveSupport() \
        .getOrCreate()
intput_dir, output_path = sys.argv[1:]
print('input_dir:', intput_dir)

full_key = "exchanges,os,countrycode,adtype,appid,packagename,campaignid,tempid,creative_id"
intput_dir = intput_dir.split(',')

df = spark.read.csv(intput_dir, sep='\t', header=True)
print('printSchema:')
df.printSchema()

#df = df.groupby(['key']).agg({'imp':'sum', 'clk':'sum', 'ins':'sum', 'rev':'sum', 'ctr':'sum', 'ivr':'sum', 'cpm':'sum'})
df = df.groupby(['key']).agg(F.sum('imp').alias('imp'), F.sum('clk').alias('clk'), F.sum('ins').alias('ins'), F.sum('rev').alias('rev'), F.sum('ctr').alias('ctr'), F.sum('ivr').alias('ivr'), F.sum('cpm').alias('cpm'))
df.printSchema()

df = df.filter("imp >= 100 and ins >= 2")
df = df.withColumn('ctr', col('clk') / col('imp'))
df = df.withColumn('ivr', col('ins') / col('imp'))
df = df.withColumn('cpm', col('rev') * 1000 / col('imp'))

df.repartition(10).write.option("header", "true").mode('overwrite').csv(output_path, sep='\t')

