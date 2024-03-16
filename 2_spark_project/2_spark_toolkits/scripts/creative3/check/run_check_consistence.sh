#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../..;pwd)
cd $root_dir

# --deploy-mode cluster \

SPARK_HOME="/data/hadoop-enviorment/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--driver-memory 8g \
--num-executors 36 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.check.RunConsisCheck \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
>& ./log/creative3/check/run_check_consistence.log