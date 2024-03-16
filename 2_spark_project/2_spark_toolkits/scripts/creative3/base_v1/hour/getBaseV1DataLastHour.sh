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

aws s3 cp s3://mob-emr-test/baihai/m_sys_model/creative3_v1/train_data_hourly/done_time_record ./log/creative3/base_v1/hour/
last_done_time=$(tail -n 1 ./log/creative3/base_v1/hour/done_time_record | awk '{print $1}')
echo "[creative3_v1 hourly]last_done_time:"$last_done_time
if [ "x" == "${last_done_time}x" ];then
    echo "[creative3_v1 hourly]Fatal: done_time_record get failed."
    email "Failed: Creative3_v1 hourly data cannot get done_time_record"
    exit 1
fi

cur_hour=`TZ=Asia/Shanghai date +"%Y%m%d%H"`
delta=-1
target_hour=`TZ=Asia/Shanghai date -d "$delta hours ago ${last_done_time:0:8} ${last_done_time:8:2}" +"%Y%m%d%H"`
while(($target_hour <= $cur_hour))
do
    real_time_path=s3://mob-emr-test/guangxue/new_base/instance/${target_hour:0:8}/${target_hour:0:10}/
    hadoop fs -test -f $real_time_path/_SUCCESS
    if [ $? -eq 0 ]; then
        echo "[creative3_v1 hourly]target_hour: $target_hour, will use $real_time_path."
        break
    fi
    let delta-=1
    target_hour=`TZ=Asia/Shanghai date -d "$delta hours ago ${last_done_time:0:8} ${last_done_time:8:2}" "+%Y%m%d%H"`
done
if [ "$target_hour" -gt "$cur_hour" ];then
    echo "[creative3_v1 hourly]Fatal: after $last_done_time, until $cur_hour, real_time_path does not exist.(the last one for checking is $real_time_path)"
    exit 1
fi

echo "[creative3_v1 hourly]execute info: "
echo "[creative3_v1 hourly]cur_hour: $cur_hour"
echo "[creative3_v1 hourly]last_done_time: $last_done_time"
echo "[creative3_v1 hourly]target_hour: $target_hour"

SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 24 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.creative3.base_v1.hour.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $target_hour \
--end_date_hour $target_hour \
>& ./log/creative3/base_v1/hour/getBaseV1DataLastHour_"$target_hour".log

if [ $? -eq 0 ]; then
  echo "$target_hour $cur_hour" >> ./log/creative3/base_v1/hour/done_time_record
  aws s3 cp ./log/creative3/base_v1/hour/done_time_record s3://mob-emr-test/baihai/m_sys_model/creative3_v1/train_data_hourly/
fi

out_path=s3://mob-emr-test/baihai/m_sys_model/creative3_v1/train_data_hourly/${target_hour:0:8}/${target_hour:0:10}
hadoop fs -test -f $out_path/_SUCCESS
if [ $? -eq 0 ]; then
    echo "[creative3_v1 hourly] Successful: Creative3_v1 Hourly Data Generation"
    email "Successful: Creative3_v1 Hourly Data Generation($target_hour)"
else
    echo "[creative3_v1 hourly] Failed: Creative3_v1 Hourly Data Generation"
    email "Failed: Creative3_v1 Hourly Data Generation($target_hour)"
fi

