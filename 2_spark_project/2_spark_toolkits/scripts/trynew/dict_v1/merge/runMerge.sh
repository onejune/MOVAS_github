#!/usr/bin/env bash

root_dir=$(cd `dirname $0`/../../../..;pwd)
cd $root_dir

if [ "x" == "${ScheduleTime}x" ]; then
    start_date_hour=`date +%Y%m%d%H`
    end_date_hour=`date +%Y%m%d%H`
else
    start_date_hour=$(date -d "$ScheduleTime" +"%Y%m%d%H")
    end_date_hour=$(date -d "$ScheduleTime" +"%Y%m%d%H")
fi

sql_step_length=100000

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 24 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.trynew.dict_v1.merge.Run \
./target/hercules-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date_hour $start_date_hour \
--end_date_hour $end_date_hour \
--sql_step_length $sql_step_length
#>& ./log/trynew/dict_v1/merge/runMerge_"$start_date_hour"_"$end_date_hour".log


out_path=s3://mob-emr-test/baihai/m_sys_model/trynew/dict/merge/${start_date_hour:0:8}/${start_date_hour:0:10}
hadoop fs -test -f $out_path/_SUCCESS
if [ $? -eq 0 ]; then
    subject="Successful: Trynew Dict Generation($start_date_hour)"
    aws s3 cp $out_path/part-00000 ./auto_try_new_dict.txt
    #cat ./auto_try_new_dict.txt > ./md5_key
    #date +%s >> ./md5_key
    md5sum auto_try_new_dict.txt > auto_try_new_dict.txt.md5
    res_path=s3://mob-emr-test/baihai/m_sys_model/trynew/dict/result/${start_date_hour:0:8}/${start_date_hour:0:10}
    aws s3 cp auto_try_new_dict.txt $res_path/
    aws s3 cp auto_try_new_dict.txt.md5 $res_path/
else
    subject="Failed: Trynew Dict Generation($start_date_hour)"
fi
python python/send_email.py "${subject}"
