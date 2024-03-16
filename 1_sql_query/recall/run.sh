#########################################################################
# File Name: query.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sun 05 Jun 2022 10:07:53 PM CST
#########################################################################
#!/bin/bash

export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf
export LANG="en_US.UTF-8"

output="s3://mob-emr-test/wanjun/ab_exp/saas_exp"
hadoop fs -rmr $output

spark-submit \
  --deploy-mode cluster \
  --num-executors 200 \
  --driver-memory 8g \
  --executor-memory 6g \
  --conf spark.sql.shuffle.partitions=1000 \
  --conf spark.default.parallelism=1000 \
  --executor-cores 2 \
  saas_rs_exp.py

output="s3://mob-emr-test/wanjun/ab_exp/saas_exp"
hadoop fs -text $output/part* > output/recall_exp.txt
