#!/usr/bin/env bash

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

sql_step_length=100000
start_date_hour="2019082220"
end_date_hour="2019082220"

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 24 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.trynew.dict_v1.merge.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $start_date_hour \
--end_date_hour $end_date_hour \
--sql_step_length $sql_step_length \
>& ./log/trynew/dict_v1/merge/runMerge_"$start_date_hour"_"$end_date_hour".log