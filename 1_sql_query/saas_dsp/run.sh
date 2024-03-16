#########################################################################
# File Name: run.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 21 Apr 2022 11:39:27 PM CST
#########################################################################
#!/bin/bash

rm -f final_cost_file
rm -f campaign_package_tag.txt

#hadoop dfs -get s3://mob-emr-test/yangjingyun/m_model_online/single_cost_by_day/final_cost_file

hadoop dfs -get s3://mob-emr-test/algorithm/AdAlgorithm/SAAS_RS/wuchui_tag/campaign_package_tag.txt

beg=2022061400
end=2022061623
#beg=2022041000
#end=2022051223
need_request=0

output="./output/ici_${beg}_${end}.txt"
sh query_tracking_data.sh $beg $end $output $need_request

final_out="./output/saas_report_${beg}_${end}.txt"
python figure_roi_tags.py $output $final_out
