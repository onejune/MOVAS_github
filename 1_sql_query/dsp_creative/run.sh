
# 开发机测试指定 k8s on 255 config
export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf

set -e

cur_hour=`date +%Y%m%d%H`
cur_day=${cur_hour:0:8}

#job所有数据输出的根目录
rootDir="s3://mob-emr-test/algorithm/AdAlgorithm/SAAS_RS/creative/adx_creative"
#小时级的汇总数据，包括imp、clk、ins、merged数据
output="$rootDir/query_data/$cur_day/$cur_hour"
#最终生成的词表的路径
result_output="$rootDir/online/$cur_day/$cur_hour"
#最终的词表名
word_table_name="adx_template_creative_rr_table.txt"

# 钉钉告警函数
function send_dingding(){
  curl 'https://oapi.dingtalk.com/robot/send?access_token=4ef2d43c4e2ef6f75fc21fa3b7974003cd44796c1c37393bcdc38d66b75c8a23' \
  -H 'Content-Type: application/json' \
  -d '{"at": {
          "atMobiles":[
              "17600977634"
          ],
          "atUserIds":[
              "yangtuan.gao"
          ],
          "isAtAll": false
      }, "text": {
          "content":"[Azkaban] adx template and creative table cal error, please check!"
      },
      "msgtype":"text"
  }'
}

function send_mysql(){
  file_name=$1
  file_num=`wc -l ./${file_name} | awk -F" " '{print $1}' - `
  file_size=`ls -lh  ./${file_name} | awk -F" "  '{print $5;}' -`
  end_time=`date "+%Y-%m-%d %H:%M:%S"`

  python mysql_report.py -b "ADX" -m "排序" -c "万军" -j "adx_creative_template_粗排词表" -f "${end_time}" -r "时间#${end_time},词表name#${file_name},条数#${file_num},大小#${file_size}"
}

# consul kv 注册
function get_json_str() {
  json_fmt='{"name":"%s","service":"%s","path":"%s","version":"%s","localfile":"%s"}\n';
  printf $json_fmt $*
}

function notify_consul() {
  train_day=$1
  key_prefix=$2
  model_name="$3"
  output_path=$4
  model_localfile=$5
  consul_kv="http://172.31.19.83:8500/v1/kv"
  localfile=$model_localfile/$(basename $output_path)
  local json_value=$(get_json_str $model_name ${model_name}-service $output_path ${train_day} $localfile)
  echo "notify_consul json_value: $json_value"
  curl -X PUT -d $json_value $consul_kv/${key_prefix}/${model_name}
}

function upload_consul(){
  model_name="$1"
  output_path="$2"
  online_path="/data/model_update/online"
  key_prefix="modeldata/ranker"
  date_str=$(date +%Y%m%d%H%M)
  notify_consul $date_str $key_prefix $model_name $output_path $online_path
}

#生成小时级的增量数据，先把imp、clk、ins分别汇总出来，然后join
now_h=`date +"%Y%m%d %H"`
beg_day=`date -d "$now_h -4 hours" +%Y%m%d%H`
end_day=`date -d "$now_h -4 hours" +%Y%m%d%H`

#spark-submit \
#    --deploy-mode cluster \
#    --executor-memory 8g \
#    --driver-memory 10g \
#    --executor-cores 2 \
#    --num-executors 400 \
#    --conf spark.dynamicAllocation.enabled=true \
#    --conf spark.dynamicAllocation.minExecutors=10 \
#    --conf spark.dynamicAllocation.maxExecutors=400 \
#    --conf spark.core.connection.ack.wait.timeout=300 \
#    pyspark_sql.py $beg_day $end_day $output/merged


set +e
#merge小时级的数据得到最终的词表
last_n_hours=24
start_date=`date -d "$now_h -$last_n_hours hours" +"%Y%m%d%H"`
end_date=`date -d "$now_h -0 hours" +"%Y%m%d%H"`
input_dir=""

#把小时级汇总的数据对应的路径拼接起来
while [[ "$start_date" -le "$end_date" ]]
do
    echo "now_date: ${start_date}"
    tomorrow=$(date -d "+1 hour ${start_date:0:8} ${start_date:8:2}" +"%Y%m%d%H")
    yestody=$(date -d "-1 hour ${start_date:0:8} ${start_date:8:2}" +"%Y%m%d%H")
 
    data_pth="$rootDir/query_data/${start_date:0:8}/${start_date:0:10}/merged"
    hadoop fs -test -f $data_pth/_SUCCESS
    if [ $? -eq 0 ]; then
        input_dir="${input_dir},${data_pth}"
    fi
    start_date=${tomorrow}
done

#去除路径列表头部多余的逗号
input_dir=${input_dir#*,}
output_table="$output/result"
echo "input_dir: $input_dir"
set -e

#开始merge小时级的词表了
spark-submit \
    --deploy-mode cluster \
    --executor-memory 6g \
    --driver-memory 8g \
    --executor-cores 2 \
    --num-executors 400 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=400 \
    --conf spark.core.connection.ack.wait.timeout=300 \
    gen_wt_table.py "$input_dir" "$output_table"

echo "output_table: $output_table"
hadoop fs -text $output_table/* > temp.dat
cat temp.dat | python post_process.py > $word_table_name

# 校验数据条数, 如果小于 50000 条 告警
line_num=`wc -l $word_table_name | awk -F" " '{print $1}'`
echo "line num: $line_num"
if [ ${line_num} -lt 50000 ];then
  echo "adx_template_creative_rr_table.txt data num is less than 50000, please check!"
  send_dingding
  exit 1
fi

#保存至s3
aws s3 cp $word_table_name $result_output/
#更新mysql消息
send_mysql $word_table_name
#update consul
model_name="adx_template_creative_rr_table"
output_path="$result_output/$word_table_name"  
upload_consul $model_name $output_path

echo "data create success!"

