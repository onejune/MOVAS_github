#!/usr/bin/env bash

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

start_date_hour=2019042300
end_date_hour=2019042300
column_name="s3://mob-emr-test/baihai/m_sys_model/creative3_v1/column_name"
combine_schema="s3://mob-emr-test/baihai/m_sys_model/creative3_v1/combine_schema_ivr"
removed_feature="s3://mob-emr-test/baihai/m_sys_model/creative3_v1/removed_feature.dat"


SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 48 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.base_v1.test.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $start_date_hour \
--end_date_hour $end_date_hour \
--column_name $column_name \
--combine_schema $combine_schema \
--removed_feature $removed_feature \
>& ./log/creative3/base_v1/test/test_"$start_date_hour"_"$end_date_hour".log

