#!/usr/bin/env bash

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

#lastDate=`TZ=Asia/Shanghai date -d "-1 day" +"%Y%m%d"`
lastDate=`TZ=Asia/Shanghai date -d "-1 day ${ScheduleTime}" +"%Y%m%d"`
out_path="s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_daily/$lastDate"
hadoop fs -test -f $out_path/_SUCCESS
if [ $? -eq 0 ]; then
    echo "[creative3_v2 daily]$out_path does already exist."
    exit 0
fi

#cur_hour=`TZ=Asia/Shanghai date +"%Y%m%d%H"`
cur_hour=`TZ=Asia/Shanghai date -d "${ScheduleTime}" +"%Y%m%d%H"`
lastDateHour="$lastDate"23
checkDateHour=$lastDateHour
delta=0
while(($checkDateHour <= $cur_hour))
do
    real_time_path=s3://mob-emr-test/guangxue/new_base/instance/${checkDateHour:0:8}/${checkDateHour:0:10}
    hadoop fs -test -f $real_time_path/_SUCCESS
    if [ $? -eq 0 ]; then
        echo "[creative3_v2 daily]$real_time_path does exist, able to generate data for $lastDate"
        break
    fi
    let delta-=1
    checkDateHour=`TZ=Asia/Shanghai date -d "$delta hours ago ${lastDateHour:0:8} ${lastDateHour:8:2}" "+%Y%m%d%H"`
done
if [[ "$checkDateHour" -gt "$cur_hour" ]];then
    echo "[creative3_v2 daily]Fatal: from $lastDateHour until $cur_hour, real time data does not exist.(last one for checking is $real_time_path)"
    exit 1
fi

#SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 48 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.update_v2.day.RunUniq \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $lastDate \
--end_date $lastDate
#>& ./log/creative3/update_v2/day/getUpdateV2DataLastDayUniq_"$lastDate".log

hadoop fs -test -f $out_path/_SUCCESS
if [ $? -eq 0 ]; then
    subject="Successful: Creative3_v2 Daily Data Generation $lastDate"
else
    subject="Failed: Creative3_v2 Daily Data Generation $lastDate"
fi
python python/send_email.py "${subject}"
