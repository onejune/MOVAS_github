export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf


function query_effect_from_tracking_template() {
    beg_date=${1}
    end_date=${2}

    unit_ids='407868,402733,389620'
    publisher_ids='10714'
    output="output/${beg_date}_${end_date}_template_from_tracking.txt"
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('us')"
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select
	split(ext_algo, ',')[15] as ext_rv_template,platform,
    concat(app_id, '#', ad_type, '#', lower(country_code), '#', ext_campaignPackageName) as key,
    lower(country_code) as country_code,
    count(*) as imp
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition
	and strategy like 'MNormalAlphaModelRankerHH%'
    group by
	split(ext_algo, ',')[15], platform,
    concat(app_id, '#', ad_type, '#', lower(country_code), '#', ext_campaignPackageName),
    lower(country_code)
    "

    ins_sql="select 
	split(ext_algo, ',')[15] as ext_rv_template,platform,
    concat(app_id, '#', ad_type, '#', lower(country_code), '#', ext_campaignPackageName) as key,
    lower(country_code) as country_code,
	sum(split(ext_algo, ',')[2]) as rev,
	count(*) as ins
    from dwh.ods_adn_trackingnew_install_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date}
	and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition
	and strategy like 'MNormalAlphaModelRankerHH%'
    group by
	split(ext_algo, ',')[15],platform,
    concat(app_id, '#', ad_type, '#', lower(country_code), '#', ext_campaignPackageName),
    lower(country_code)
    "

    sql="select a.*, b.ins, b.rev
    from ($imp_sql)a left join ($ins_sql)b
    on a.ext_rv_template=b.ext_rv_template and a.key=b.key and a.platform=b.platform
	where a.imp>500 and b.ins>10
    "

    hive_out="s3://mob-emr-test/wanjun/hive_out/creative_info_table/tpl_${beg_date}_${end_date}"
	hadoop fs -rmr $hive_out
    echo "sql:$sql"

    spark-submit \
    --deploy-mode cluster \
    --num-executors 50 \
    --executor-cores 2 \
    --driver-memory 4G \
    --executor-memory 4G \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=100 \
    --conf spark.core.connection.ack.wait.timeout=300 \
    --class org.mobvista.dataplatform.SubmitSparkSql s3://mob-emr/adn/k8s_spark_migrate/release/spark-sql-submitter-1.0.0.jar \
    "${sql}"  \
    "${hive_out}"  \
    "1" \
    Overwrite \
    csv \

    hadoop fs -text $hive_out/* > output/tpl_cpm.txt

    echo ${hive_out}
}

query_effect_from_tracking_template 2022030800 2022031023
