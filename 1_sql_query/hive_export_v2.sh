#########################################################################
# File Name: hive_export_v2.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Fri 11 Feb 2022 01:41:27 PM CST
#########################################################################
#!/bin/bash
#!/bin/bash
# 从hive表中到出数据生成文件
# params hive_sql hive查询语句
# params local_path 数据文件本地存放目录
# params s3_path 数据文件S3存放目录

if [ "" == "${1}" ]; then
    echo "hive_sql can not be empty"
    exit 1
fi
hive_sql=${1}
if [ "" == "${2}" ]; then
    echo "s3_path can not be empty"
    exit 1
fi
s3_path=${2}

hadoop fs -rm -r ${s3_path}
##根据作业传入参数 group_by_spark 决定是否spark SQL

aws s3 cp s3://super-mtg-report/adn/software/jar/erised-sql-3.9.0_spark.jar ./output/jar/
jar_file=./output/jar/erised-sql-3.9.0_spark.jar
pwd
current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s`
#将current转换为时间戳，精确到毫秒
currentTimeStamp=$((timeStamp*1000+`date "+%N"`/1000000))
 hive_sql="${hive_sql//\`/\\\`}"
echo "\"${hive_sql}\""  > ${currentTimeStamp}.sql
mkdir -p ./output/sql/
mv ${currentTimeStamp}.sql ./output/sql/
cat ./output/sql/${currentTimeStamp}.sql
sqlpath=./output/sql/${currentTimeStamp}.sql
if [[ "" != "${s3_path}" ]]; then
    echo $hive_sql
    spark-submit --name "wanjun" --class com.mobvista.adn.spark.SqlOnSpark --master yarn --deploy-mode cluster --num-executors 30 --total-executor-cores 3 --executor-cores 3 --executor-memory 5G --conf spark.dynamicAllocation.enabled=false --conf spark.default.parallelism=1000 ${jar_file} `cat ${sqlpath}` ${s3_path}
#        eval "${cmd}"
else
    spark-sql --num-executors 100 -e "${hive_sql}"
fi

if [ $? -ne 0 ]; then
    exit 1
fi

hadoop dfs -touchz ${s3_path}_SUCCESS
if [ $? -ne 0 ]; then
  exit 1 
fi

aws s3 ls ${s3_path}

