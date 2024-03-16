#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../../..;pwd)
cd $root_dir

min_id=0
max_id=270000000
sql_length=30000000
sql_step_length=100000
for start_id in `seq $min_id $sql_length $max_id`;
do

    end_id=`expr $start_id + $sql_length`
    SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 36 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.trynew.dict_v1.campaign_list.base.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_id $start_id \
--end_id $end_id \
--sql_step_length $sql_step_length \
>& ./log/trynew/dict_v1/campaign_list/base/getCamListBase_id"$start_id"_id"$end_id".log

done