#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

rm -f done_time_record
aws s3 cp s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/done_time_record .
last_done_time=`cat done_time_record`
if [ "x" == "${last_done_time}x" ];then
    exit 1
fi

cur_time=`TZ=Asia/Shanghai date -d "-7 hours ${ScheduleTime}" +"%s"`
target_time=$last_done_time
while true
do
    ((target_time=target_time+3600))
    if [ $target_time -gt $cur_time ]; then
        break
    fi

    target_time_str=`date -d @$target_time +"%Y%m%d%H"`
    post_process_path="s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process/${target_time_str:0:8}/$target_time_str"
    hadoop fs -test -f $post_process_path/_SUCCESS
    if [ $? -eq 0 ]; then
        spark-submit \
            --master yarn \
            --deploy-mode cluster \
            --driver-memory 8g \
            --num-executors 48 \
            --executor-cores 4 \
            --executor-memory 8g \
            --files $SPARK_HOME/conf/hive-site.xml \
            --jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
            --class com.mobvista.data.creative3.exp1.join_label.RunJoinLabel \
            ./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
            --start_date_hour $target_time_str \
            --end_date_hour $target_time_str
        join_path="s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/${target_time_str:0:8}/${target_time_str:0:10}"
        hadoop fs -test -f $join_path/_SUCCESS
        if [ $? -eq 0 ]; then
            last_done_time=$target_time
        fi
    fi
done

echo "$last_done_time" > done_time_record
hadoop dfs -rmr s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/done_time_record
sleep 5s
aws s3 cp done_time_record s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/
