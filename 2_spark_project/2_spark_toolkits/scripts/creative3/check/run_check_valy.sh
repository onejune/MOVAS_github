#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../..;pwd)
cd $root_dir

time_list=(2019071018 2019071118 2019071218 2019071318 2019071418)
for target_time_str in ${time_list[@]};
do

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 64 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.check.RunCheckValyria \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $target_time_str \
--end_date_hour $target_time_str \
>& ./log/creative3/check/run_check_valy_"$target_time_str".log

done