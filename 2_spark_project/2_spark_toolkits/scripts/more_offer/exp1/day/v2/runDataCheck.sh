#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../../..;pwd)
cd $root_dir

startDate="20190822"
endDate="20190822"

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 48 \
--executor-cores 4 \
--executor-memory 6g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.more_offer.exp1.day.v2.RunDataCheck \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $startDate \
--end_date $endDate \
>& ./log/more_offer/exp1/day/v2/runDataCheck_"$startDate"_"$endDate".log
