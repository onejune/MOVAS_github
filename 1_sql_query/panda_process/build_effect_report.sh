function query_hourly_roi() {
    begin_date=$1
    end_date=$2
    output=$3

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition="is not null"
    sql="select substr(req_time,1,8) as dtm, ad_type, 
	country_code,
	unit_id,
	is_hb,
    bucket,
	platform,
    sum(total_req) as req, 
	sum(adx_impression) as adx_imp,
	sum(impression) as nor_imp,
	sum(impression)+sum(adx_impression) as impression,
	sum(click) as click,
	sum(normal_install) as install,
	sum(adx_revenue) as adx_rev,
	sum(normal_revenue) as nor_rev,
    sum(adx_revenue)+sum(normal_revenue) as revenue,
	sum(cost) as nor_cost,
	sum(adx_cost) as adx_cost,
    sum(cost)+sum(adx_cost) as cost
    from monitor_effect.model_effect_info_test 
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
    and req_time>='$begin_date' and req_time<='$end_date'
	and unit_id $unit_id_condition
	group by substr(req_time,1,8), ad_type, 
	platform,
    country_code,
	unit_id,
	bucket, 
	is_hb
	having sum(impression)+sum(adx_impression)>0;"

    echo "$sql"
    hive -e "$sql" >$output
}

del_date=$(date +"%Y%m%d" -d "5 days ago")
rm -rf output/whole_day_effect/*${del_date}*
rm -rf output/latest_6_hour/*${del_date}*
rm -rf ./temp/*

cal_date=$(date +"%Y%m%d%H%M")
cur_hour=${cal_date:8:2}
if [ $cur_hour -lt 4 ]; then
    cal_date=$(date +"%Y%m%d%H%M" -d "1 days ago")
fi

aws s3 cp s3://mob-emr-test/aresyang/modeldata/ranker/m_ftrl_config/m_ftrl_config.dat ./conf/

echo "query data from hive......"
echo "cal_date:$cal_date"
record_time=$(date +"%Y-%m-%d %H:%M:%S")

today_begin=${cal_date:0:8}00
today_end=$(date +"%Y%m%d%H" -d "0 days ago")
query_hourly_roi $today_begin $today_end temp/whole_day_effect.dat &

h_begin=$(date +"%Y%m%d%H" -d "4 hour ago")
h_end=$(date +"%Y%m%d%H" -d "0 days ago")
query_hourly_roi $h_begin $h_end temp/latest6_hour_effect.dat &

wait

echo "generate groupby data......"
python model_effect_report.py temp/whole_day_effect.dat whole_day_effect
python model_effect_report.py temp/latest6_hour_effect.dat latest6_hour_effect

echo "format effect report......"
echo "-------------whole day effect---------------"

tag="whole_day_effect"
output="output/whole_day_effect/model_effect_req_time_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/total.$tag
cat temp/total.$tag.format >>$output

output="output/whole_day_effect/us_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/us.$tag
cat temp/us.$tag.format >>$output

output="output/whole_day_effect/cn_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/cn.$tag
cat temp/cn.$tag.format >>$output

output="output/whole_day_effect/rviv_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/rviv.$tag
cat temp/rviv.$tag.format >>$output

output="output/whole_day_effect/hb_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/hb.$tag
cat temp/hb.$tag.format >>$output

output="output/whole_day_effect/top_unit_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/top_unit.$tag
cat temp/top_unit.$tag.format >>$output

echo "-------------latest 6 hours effect---------------"
tag="latest6_hour_effect"
output="output/latest_6_hour/model_effect_req_time_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/total.$tag
cat temp/total.$tag.format >>$output

output="output/latest_6_hour/us_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/us.$tag
cat temp/us.$tag.format >>$output

output="output/latest_6_hour/cn_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/cn.$tag
cat temp/cn.$tag.format >>$output

output="output/latest_6_hour/rviv_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/rviv.$tag
cat temp/rviv.$tag.format >>$output

output="output/latest_6_hour/hb_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/hb.$tag
cat temp/hb.$tag.format >>$output

output="output/latest_6_hour/top_unit_effect_${cal_date:0:8}"
echo $record_time >>$output
python process_result.py temp/top_unit.$tag
cat temp/top_unit.$tag.format >>$output
