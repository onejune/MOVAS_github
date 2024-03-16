#!/usr/bin/env bash

##########################  function(start)  ######################################

#email()
function email(){

    email_reciver="hai.bai@mintegral.com"
    email_sender="baihai922@163.com"
    email_username="baihai922"
    email_password="haigelisi0922"

    email_title=$1
    email_content=$1

    ./bin/sendEmail -f ${email_sender} -t ${email_reciver} -u ${email_title} -s smtp.163.com -xu ${email_username} -xp ${email_password} -m ${email_content} -o message-charset=utf-8 -o tls=no
}

##########################  function(end)  ######################################

source ~/.bash_profile
source ~/.bashrc

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

lastDate=`TZ=Asia/Shanghai date -d "-1 day" +"%Y%m%d"`

flag=1
iv_out_path=s3://mob-emr-test/wanjun/m_sys_model/iv_train_data/$lastDate
hadoop fs -test -f $iv_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    echo "[creative3_v1 daily]$iv_out_path does not exist."
    flag=0
fi
rv_out_path=s3://mob-emr-test/wanjun/m_sys_model/rv_train_data/$lastDate
hadoop fs -test -f $rv_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    echo "[creative3_v1 daily]$rv_out_path does not exist."
    flag=0
fi
native_out_path=s3://mob-emr-test/wanjun/m_sys_model/native_train_data_0.1/$lastDate
hadoop fs -test -f $native_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    echo "[creative3_v1 daily]$native_out_path does not exist."
    flag=0
fi

if [ "$flag" -eq 1 ]; then
    echo "[creative3_v1 daily]all needed data does already exist."
    exit 0
fi

cur_hour=`TZ=Asia/Shanghai date +"%Y%m%d%H"`
lastDateHour="$lastDate"23
checkDateHour=$lastDateHour
delta=0
while(($checkDateHour <= $cur_hour))
do
    real_time_path=s3://mob-emr-test/guangxue/real_time_mwho/${checkDateHour:0:8}/${checkDateHour:0:10}
    hadoop fs -test -f $real_time_path/_SUCCESS
    if [ $? -eq 0 ]; then
        echo "[creative3_v1 daily]$real_time_path does exist, able to generate data for $lastDate"
        break
    fi
    let delta-=1
    checkDateHour=`TZ=Asia/Shanghai date -d "$delta hours ago ${lastDateHour:0:8} ${lastDateHour:8:2}" "+%Y%m%d%H"`
done
if [[ "$checkDateHour" -gt "$cur_hour" ]];then
    echo "[creative3_v1 daily]Fatal: from $lastDateHour until $cur_hour, real time data does not exist.(last one for checking is $real_time_path)"
    exit 1
fi

SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 48 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.base_v1.day.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $lastDate \
--end_date $lastDate \
>& ./log/creative3/base_v1/day/getBaseV1DataLastDay_"$lastDate".log

flag=1
hadoop fs -test -f $iv_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    flag=0
fi
hadoop fs -test -f $rv_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    flag=0
fi
hadoop fs -test -f $native_out_path/_SUCCESS
if [ $? -ne 0 ]; then
    flag=0
fi
if [ "$flag" -eq 1 ]; then
  email "Successful: Creative3_v1 Daily Data Generation $lastDate"
else
  email "Failed: Creative3_v1 Daily Data Generation $lastDate"
fi
