#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../../..;pwd)
cd $root_dir

date_hour=`date "+%Y%m%d%H"`
sql_step_length=100000

#SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
SPARK_HOME="/data/hadoop-enviorment/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 36 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.trynew.dict_v1.campaign_list.base2increment.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--date_hour $date_hour \
--sql_step_length $sql_step_length \
>& ./log/trynew/dict_v1/campaign_list/base2increment/getCamListBase2Incre_"$date_hour".log
