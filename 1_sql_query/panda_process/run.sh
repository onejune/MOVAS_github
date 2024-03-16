#########################################################################
# File Name: run.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Mon 05 Apr 2021 09:27:33 PM CST
#########################################################################
#!/bin/bash

if [ ! "$1" ]; then 
	echo "Error: input file is null, system exit......"
	exit 1
fi

input_file=$1
hive_temp=$input_file.csv
output_file="result.txt"

echo "input_file: $input_file lines: $( wc -l $input_file )"

if [ ! -s $hive_temp ]; then
	echo "generate csv file......"
	sed 's/[\t]/,/g' $input_file > $hive_temp
fi

python model_effect_report.py $hive_temp $output_file
