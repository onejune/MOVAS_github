#!/bin/sh
source ./get_done_time_record.sh

hadoop fs -get -f oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data/done_time_record done_time_record
done_hour=`cat done_time_record`
tmp_hour="${done_hour:0:4}-${done_hour:4:2}-${done_hour:6:2} ${done_hour:8:2}"
start_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
end_hour=$(get_done_time_record "oss://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process_tag")

end_day="${end_hour:0:8}"
if [ -z $done_hour ] || [ -z $end_hour ];then
  echo "done_hour or end_hour is NULL: done_hour=$done_hour, end_hour=$end_hour"
  exit 1
fi
if [ $end_hour -lt $start_hour ];then
  echo "data already updated: $end_hour"
  exit 0
fi
output_path="oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data/${end_day}/${end_hour}"
hadoop fs -rm -r $output_path
hadoop fs -mkdir -p oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data/${end_day}

spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--driver-memory 4g \
	--num-executors 50 \
	--executor-cores 2 \
	--executor-memory 4g \
	--conf spark.executor.memoryOverhead=1024 \
	--files ${HIVE_CONF_DIR}/hive-site.xml,oss://mob-emr-test/jiangnan/conf/kudu/kudu-column-conf.yml,oss://mob-emr-test/lin.zhao/m_model_online/fm_column_sourceid/column_name_lr.yml \
	--jars oss://mob-emr-test/kehan/myjar/snakeyaml-1.23.jar,oss://mob-emr-test/kehan/myjar/argparse4j-0.8.1.jar,oss://mob-emr-test/jiangnan/lib/dataflow-sdk-1.0.jar,oss://mob-emr-test/jiangnan/jars/kudu-client-1.10.0.jar,oss://mob-emr-test/jiangnan/jars/kudu-spark2_2.11-1.10.0.jar \
	--class com.mobvista.train_data_flow.task.kudu_data_for_fm.KuduDataForFM \
	--name "KuduDataForFM" \
        ./dataflow-job-1.0.jar ${start_hour} ${end_hour} ${output_path} || exit 1


if [ $? -eq 0 ];then
  echo $end_hour > done_time_record
  hadoop fs -put -f done_time_record oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data/done_time_record
  rm -rf done_time_record
  # delete data
  two_month_ago=`date -d "$end_day -2 months" +%Y%m%d`
  hadoop fs -rm -r oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data/$two_month_ago
  exit 0
fi
