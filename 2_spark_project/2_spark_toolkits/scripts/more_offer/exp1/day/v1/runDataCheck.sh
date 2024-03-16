#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../../..;pwd)
cd $root_dir

startDate="20190812"
for i in `seq 1 14`;
do

	targetDate=`date -d "-$i day $startDate" "+%Y%m%d"`
	echo $targetDate

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 48 \
--executor-cores 4 \
--executor-memory 6g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.more_offer.exp1.day.v1.RunDataCheck \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $targetDate \
--end_date $targetDate \
>& ./log/more_offer/exp1/day/v1/runDataCheck_"$targetDate"_"$targetDate".log

done