#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir


start_date_hour=2019070218
end_date_hour=2019070305


    spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --num-executors 48 \
    --executor-cores 4 \
    --executor-memory 8g \
    --files $SPARK_HOME/conf/hive-site.xml \
    --jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
    --class com.mobvista.data.creative3.exp1.extract_cpc_label.RunExtractCpcLabel \
    ./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --start_date_hour $start_date_hour \
    --end_date_hour $end_date_hour \
    >& log/creative3/exp1/extract_cpc_label/supExtractCpcLabel_"$start_date_hour"_"$end_date_hour".log


