#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir


aws s3 cp s3://mob-emr-test/baihai/m_sys_model/creative_model_exp1/train_data_hourly/done_time_record ./log/creative_model/exp1/hour/
last_done_time=$(tail -n 1 ./log/creative_model/exp1/hour/done_time_record | awk '{print $1}')
echo "[creative_model hourly]last_done_time:"$last_done_time
if [ "x" == "${last_done_time}x" ];then
    echo "[creative_model hourly]Fatal: done_time_record get failed."
    email "Failed: creative_model hourly data cannot get done_time_record"
    exit 1
fi

#cur_hour=`TZ=Asia/Shanghai date +"%Y%m%d%H"`
cur_hour=`TZ=Asia/Shanghai date -d "${ScheduleTime}" +"%Y%m%d%H"`
delta=-1
target_hour=`TZ=Asia/Shanghai date -d "$delta hours ago ${last_done_time:0:8} ${last_done_time:8:2}" +"%Y%m%d%H"`
while(($target_hour <= $cur_hour))
do
    creative3_v2_path=s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/${target_hour:0:8}/${target_hour:0:10}/
    hadoop fs -test -f $creative3_v2_path/_SUCCESS
    if [ $? -eq 0 ]; then
        echo "[creative_model hourly]target_hour: $target_hour, will use $creative3_v2_path."
        break
    fi
    let delta-=1
    target_hour=`TZ=Asia/Shanghai date -d "$delta hours ago ${last_done_time:0:8} ${last_done_time:8:2}" "+%Y%m%d%H"`
done
if [ "$target_hour" -gt "$cur_hour" ];then
    echo "[creative_model hourly]Fatal: after $last_done_time, until $cur_hour, creative3_v2_path does not exist.(the last one for checking is $creative3_v2_path)"
    exit 1
fi

echo "[creative_model hourly]execute info: "
echo "[creative_model hourly]cur_hour: $cur_hour"
echo "[creative_model hourly]last_done_time: $last_done_time"
echo "[creative_model hourly]target_hour: $target_hour"

#SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 24 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative_model.exp1.hour.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $target_hour \
--end_date_hour $target_hour
#>& ./log/creative_model/exp1/hour/getCreativeModelDataLastHour_"$target_hour".log

if [ $? -eq 0 ]; then
  echo "$target_hour $cur_hour" >> ./log/creative_model/exp1/hour/done_time_record
  aws s3 cp ./log/creative_model/exp1/hour/done_time_record s3://mob-emr-test/baihai/m_sys_model/creative_model_exp1/train_data_hourly/
fi

out_path=s3://mob-emr-test/baihai/m_sys_model/creative_model_exp1/train_data_hourly/${target_hour:0:8}/${target_hour:0:10}
hadoop fs -test -f $out_path/_SUCCESS
if [ $? -eq 0 ]; then
    subject="Successful: creative_model Hourly Data Generation($target_hour)"
else
    subject="Failed: creative_model Hourly Data Generation($target_hour)"
fi
python python/send_email.py "${subject}"