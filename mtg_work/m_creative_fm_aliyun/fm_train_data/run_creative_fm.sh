source ~/.bashrc

input_prefix="oss://mob-emr-test/lin.zhao/m_model_online/fm_raw_data"
output_prefix="oss://mob-emr-test/lin.zhao/m_model_online/fm_train_data"

function collect_data() {
  local start_hour=$1
  local end_hour=$2
  tmp_hour="${start_hour:0:4}-${start_hour:4:2}-${start_hour:6:2} ${start_hour:8:2}"
  local cur_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
  while [ $cur_hour -le $end_hour ]
  do
    hadoop fs -ls $input_prefix/${cur_hour:0:8}/$cur_hour/_SUCCESS
    if [ $? -eq 0 ];then
      input_path+="$input_prefix/${cur_hour:0:8}/$cur_hour "
    fi
    tmp_hour="${cur_hour:0:4}-${cur_hour:4:2}-${cur_hour:6:2} ${cur_hour:8:2}"
    cur_hour=`date -d "+1 hour $tmp_hour" +%Y%m%d%H`
  done
  # echo $input_path
}

start_hour=`hadoop fs -cat $output_prefix/done_time_record`
end_hour=`hadoop fs -cat $input_prefix/done_time_record`
if [ -z $start_hour ] || [ -z $end_hour ];then
  echo "start_hour or end_hour is NULL: start_hour=$start_hour, end_hour=$end_hour"
  exit 1
fi
if [ $end_hour -le $start_hour ];then
  echo "data already updated: $end_hour"
  exit 0
fi
input_path=""
collect_data $start_hour $end_hour
echo $input_path
output_path="$output_prefix/${end_hour:0:8}/$end_hour"
hadoop fs -mkdir $output_prefix/${end_hour:0:8}
hadoop fs -rm -r $output_path
sh -x merge.sh "$input_path" "$output_path"

if [ $? -ne 0 ];then
  echo "merge error: $end_hour"
  exit 1
fi
echo $end_hour > done_time_record
hadoop fs -put -f done_time_record ${output_prefix}/done_time_record
rm -rf done_time_record
# delete data
one_month_ago=`date -d "${end_hour:0:8} -1 months" +%Y%m%d`
hadoop fs -rm -r ${output_prefix}/${one_month_ago}
exit 0

