#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

start_date_hour=2019060500
end_date_hour=2019060506

SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 24 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.update_v2.hour.RunUniq \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $start_date_hour \
--end_date_hour $end_date_hour \
>& ./log/creative3/update_v2/hour/supUpdateV2DataLastHour_"$start_date_hour"_"$end_date_hour".log