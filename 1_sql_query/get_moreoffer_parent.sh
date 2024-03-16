#########################################################################
# File Name: get_moreoffer_parent.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sun 07 Aug 2022 07:23:58 PM CST
#########################################################################
#!/bin/bash
export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf

beg_date=$1
end_date=$2
hive_out="s3://mob-emr-test/wanjun/hive/mf_parent_${beg_date}_${end_date}"

sql="
select 
    app_id,
    unit_id as moreOffer_unitid,
    get_json_object(ext_data2,'$.p_ad_t') as mainOffer_adtype,
    get_json_object(ext_data2,'$.parent_id') as mainOffer_unitid,
    count(*) as moreOffer_imp
from dwh.ods_adn_trackingnew_impression 
where concat(yyyy,mm,dd,hh)>='${beg_date}' and concat(yyyy,mm,dd,hh)<='${end_date}'
    and  ad_type in ('more_offer')
    and  get_json_object(ext_data2,'$.p_ad_t') in ('sdk_banner', 'rewarded_video', 'interstitial_video')
group by
    app_id,
    unit_id,
    get_json_object(ext_data2,'$.p_ad_t'),
    get_json_object(ext_data2,'$.parent_id')

"

echo ${sql}

echo ${hive_out}


spark-submit \
--deploy-mode cluster \
--num-executors 300 \
--executor-cores 4 \
--driver-cores 4 \
--driver-memory 8G \
--executor-memory 8G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=300 \
--conf spark.core.connection.ack.wait.timeout=300 \
--class org.mobvista.dataplatform.SubmitSparkSql s3://mob-emr/adn/k8s_spark_migrate/release/spark-sql-submitter-1.0.0.jar \
"${sql}"  \
"${hive_out}"  \
"1" \
Overwrite \
csv \
"\t" \
"true" \

echo ${hive_out}

