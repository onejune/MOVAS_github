#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

#start_date=`date -d "-1 day" "+%Y%m%d"`
start_date=20191008
#end_date=`date -d "-1 day" "+%Y%m%d"`
end_date=20191008

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--driver-memory 8g \
--num-executors 100 \
--executor-cores 6 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.mvp_mining.exp1.features.RunUserFeatureGeneration \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $start_date \
--end_date $end_date \
>& ./log/mvp_mining/exp1/features/userFeatureGeneration_"$start_date"_"$end_date".log

