#!/bin/sh

# aws s3 cp s3://mob-emr-test/lin.zhao/m_model_online/fm_raw_data_tmp/done_time_record done_time_record
# done_hour=`cat done_time_record`
# tmp_hour="${done_hour:0:4}-${done_hour:4:2}-${done_hour:6:2} ${done_hour:8:2}"
# start_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
# end_time=`hadoop fs -cat s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process_tag/done_time_record`
# end_hour=`date -d @$end_time +%Y%m%d%H`
start_hour=$1
end_hour=$2
end_day="${end_hour:0:8}"
hadoop fs -ls s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process_tag/$end_day/$end_hour/_SUCCESS
if [ $end_hour -lt $start_hour ];then
  echo "data already updated: $end_hour"
  exit 0
fi
output_path="s3://mob-emr-test/lin.zhao/m_model_online/fm_raw_data_tmp/${end_day}/${end_hour}"
hadoop fs -rm -r $output_path
hadoop fs -mkdir -p s3://mob-emr-test/lin.zhao/m_model_online/fm_raw_data_tmp/${end_day}

spark-submit --class com.mobvista.train_data_flow.task.kudu_data_for_fm.KuduDataForFM \
	--conf spark.sql.shuffle.partitions=1000 \
	--conf spark.default.parallelism=1000 \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--name "KuduDataForFM" \
	--files ${SPARK_CONF_DIR}/hive-site.xml,s3://mob-emr-test/jiangnan/conf/kudu/kudu-column-conf.yml,s3://mob-emr-test/lin.zhao/m_model_online/fm_column/column_name_lr.yml \
        --jars s3://mob-emr-test/jiangnan/lib/dataflow-sdk-1.0.jar,s3://mob-emr-test/kehan/myjar/snakeyaml-1.23.jar,s3://mob-emr-test/kehan/myjar/argparse4j-0.8.1.jar,s3://mob-emr-test/jiangnan/jars/kudu-client-1.12.0.jar \
	--master yarn \
	--deploy-mode cluster \
	--executor-memory 4G \
	--driver-memory 1G \
	--executor-cores 2 \
	--num-executors 25 \
	./dataflow-job-1.0.jar ${start_hour} ${end_hour} ${output_path} || exit 1

if [ $? -eq 0 ];then
  echo "success: $end_hour"
  # echo $end_hour > done_time_record
  # aws s3 cp done_time_record s3://mob-emr-test/lin.zhao/m_model_online/fm_raw_data_tmp/done_time_record
  # rm -rf done_time_record
  # # delete data
  # two_month_ago=`date -d "$end_day -2 months" +%Y%m%d`
  # hadoop fs -rm -r s3://mob-emr-test/lin.zhao/m_model_online/fm_raw_data_tmp/$two_month_ago
  # exit 0
fi
