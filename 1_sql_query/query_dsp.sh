#########################################################################
# File Name: query_tracking_data.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 26 Apr 2022 07:46:22 AM CST
#########################################################################
#!/bin/bash

export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf
export LANG="en_US.UTF-8"

function spark_submit_job() {
    sql=$1
    tag=$2
    output=$3
    hive_out="s3://mob-emr-test/wanjun/hive_out/temp_temp/$tag"
    if [ -z "$sql" ] || [ -z "$tag" ] || [ -z "$output" ]; then
        echo "ERROR: input is valid, exit !"
        exit
    fi
    hadoop fs -rmr $hive_out
    echo "sql: $sql"

    spark-submit \
    --deploy-mode cluster \
    --num-executors 400 \
    --executor-cores 2 \
    --driver-cores 4 \
    --driver-memory 12G \
    --executor-memory 6G \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=500 \
    --conf spark.core.connection.ack.wait.timeout=600 \
    --class org.mobvista.dataplatform.SubmitSparkSql s3://mob-emr/adn/k8s_spark_migrate/release/spark-sql-submitter-1.0.0.jar \
    "${sql}"  \
    "${hive_out}"  \
    "1" \
    Overwrite \
    csv \
    "," \
    true

    hadoop fs -text $hive_out/* > $output
}

begin_date=2022052300
end_date=2022052323

sql="
    select exchanges, concat(yr,mt,dt,hh) as dtm, get_json_object(ext10,'$.reqtype') as ad_type,
    count (*) as imp,
    sum(price) as cost
    from adn_dsp.log_adn_dsp_impression_hour
    where concat(yr,mt,dt,hh) between '$begin_date' and '$end_date'
    and ext2='rank_model_camp'
    and ext8 not like '%TC%'
    and ext8 not like '%RTDSP%'
    and get_json_object(ext10,'$.reqtype') in ('vin','vre')
    group by
    exchanges,
    concat(yr,mt,dt,hh), get_json_object(ext10,'$.reqtype')
"

tag="query_imp_price"
output="./output/dsp_imp_price.txt"
spark_submit_job "$sql" $tag $output


