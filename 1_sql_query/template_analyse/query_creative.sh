export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf




beg_date=20220308
end_date=20220310

hive_out="s3://mob-emr-test/wanjun/hive_out/creative_info_table/tpl_${beg_date}_${end_date}"

sql="
	select concat_ws('#',app_id, ad_type, country_code, packagename) as key, 
	videotemplate, 
	sum(impression) as imp,
	sum(install) as ins,
	sum(revenue)*1000/sum(impression) as cpm
	from  monitor_effect.creative_info_table 
	where concat(year,month,day)>=$beg_date and concat(year,month,day)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video') 
	and bucket_id like '1_%'
	and country_code in('us')
	group by concat_ws('#',app_id, ad_type, country_code, packagename), 
	videotemplate
	having imp>500 and ins>10
;"

echo ${sql}

echo ${hive_out}


spark-submit \
--deploy-mode cluster \
--num-executors 10 \
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

hadoop fs -text $hive_out/* > output/tpl.txt

echo ${hive_out}
