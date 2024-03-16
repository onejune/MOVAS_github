#!/bin/bash

input_path=$1
output_path=$2
hadoop fs -rm -r $output_path
job_name="m_data_fm_new"

hadoop-streaming \
-D mapreduce.job.name="${job_name}" \
-D mapreduce.map.memory.mb=6096 \
-D mapred.reduce.tasks=10 \
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
-input ${input_path} \
-output ${output_path} \
-file merge_mapper.py \
-file merge_reducer.py \
-file bkdrEncode.so \
-file column_name_lr \
-file combine_schema_fm \
-mapper "python merge_mapper.py column_name_lr combine_schema_fm" \
-reducer "python merge_reducer.py"

#-cacheArchive s3://mob-emr/data/will/tools/python272.tar.gz#python27 \
if [ $? -ne 0 ]; then 
    exit 1
else
    hadoop fs -touchz ${output_path}/_SUCCESS
fi
