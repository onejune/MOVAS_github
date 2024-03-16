#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../..;pwd)
cd $root_dir

start_date=20190715
end_date=20190722

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 50 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.check.RunPredictionCheck \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $start_date \
--end_date $end_date \
>& ./log/creative3/check/run_check_prediction_"$start_date"_"$end_date".log
