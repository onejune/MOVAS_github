source ~/.bashrc

input_prefix="oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data"
output_prefix="oss://mob-emr-test/lin.zhao/m_model_online/fm_train_data"

function collect_data() {
  local start_hour=$1
  local end_hour=$2
  local input_path=""
  tmp_hour="${start_hour:0:4}-${start_hour:4:2}-${start_hour:6:2} ${start_hour:8:2}"
  local cur_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
  while [ $cur_hour -le $end_hour ]
  do
    input_path+="$input_prefix/${cur_hour:0:8}/$cur_hour "
    tmp_hour="${cur_hour:0:4}-${cur_hour:4:2}-${cur_hour:6:2} ${cur_hour:8:2}"
    cur_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
  done
  echo $input_path
}

# start_hour=`hadoop fs -cat $output_prefix/done_time_record`
# end_hour=`hadoop fs -cat $input_prefix/done_time_record`
end_hour=$1

# if [ $end_hour -le $start_hour ];then
#   echo "data already updated: $end_hour"
#   exit 0
# fi
# input_path=`collect_data $start_hour $end_hour`
input_path="$input_prefix/${end_hour:0:8}/$end_hour"
output_path="$output_prefix/${end_hour:0:8}/$end_hour"
# aws oss cp oss://mob-emr-test/yangjingyun/m_sys_model/creative_optimize/offline_data/final_source_id final_source_id
# sourceid_path=`hadoop fs -cat oss://mob-emr-test/yangjingyun/m_sys_model/creative_optimize/offline_data/data_pointer_for_source_id`
# aws oss cp $sourceid_path final_source_id
hadoop fs -mkdir -p $output_prefix/${end_hour:0:8}
sh -x merge.sh "$input_path" "$output_path"

hadoop fs -ls ${output_path}/_SUCCESS
if [ $? -eq 0 ];then
  echo $end_hour > done_time_record
  # aws oss cp done_time_record ${output_prefix}/done_time_record
  rm -rf *time_record
  echo "SUCCESS: ${end_hour}"
  # # delete data
  # one_month_ago=`date -d "${end_hour:0:8} -1 months" +%Y%m%d`
  # hadoop fs -rm -r ${output_prefix}/${one_month_ago}
  # exit 0
fi
# echo "FAILURE: ${end_hour}"

