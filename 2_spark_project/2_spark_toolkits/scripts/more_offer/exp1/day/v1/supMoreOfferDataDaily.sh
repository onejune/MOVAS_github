#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../../..;pwd)
cd $root_dir

start_date=20190729
end_date=20190729

#SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 64 \
--executor-cores 4 \
--executor-memory 6g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.more_offer.exp1.day.v1.RunJoinData \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $start_date \
--end_date $end_date \
>& ./log/more_offer/exp1/day/v1/joinData_"$start_date"_"$end_date".log
