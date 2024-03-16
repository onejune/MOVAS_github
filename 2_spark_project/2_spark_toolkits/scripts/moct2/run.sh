#source ~/.bash_profile
#source ~/.bashrc

root_dir=$(cd `dirname $0`/../..;pwd)
echo "root_dir=$root_dir"
cd $root_dir

start_date="20200228"
end_date="20200228"

SPARK_HOME="/data/hadoop-home/hdp-spark-2.3.1"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--num-executors 32 \
--executor-cores 4 \
--executor-memory 8g \
--files $SPARK_HOME/conf/hive-site.xml \
--jars s3://mob-emr-test/xujian/jars/Common-SerDe-1.0-SNAPSHOT.jar,$(for file in jars/*.jar;do echo -n $file,; done) \
--class com.mobvista.data.moct2.train_data.Run \
./target/2_spark_toolkits-1.0-SNAPSHOT-jar-with-dependencies.jar \
--start_date $start_date \
--end_date $end_date \
>& ./log/moct2_"$start_date"_"$end_date".log
