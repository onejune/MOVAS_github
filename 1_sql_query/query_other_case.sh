#########################################################################
# File Name: query_other_case.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 18 Nov 2021 11:59:00 AM CST
#########################################################################
#!/bin/bash
function query_hb_nps_time() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition="is not null"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, algorithm, 
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    platform, app_id,
    split(split(ext_algo, ',')[45], '\004')[0] as region,
    split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
    sum(split(split(ext_algo, ',')[33], '\004')[0]) as sum_time_use,
    sum(split(split(ext_algo, ',')[33], '\004')[1]) as sum_task_cnt,
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and ad_type='sdk_banner'
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, algorithm, 
    split(split(strategy, '\\;')[7], '-')[1],
    platform, app_id,
    split(split(ext_algo, ',')[45], '\004')[0],
    if(lower(country_code) in('cn','us'),lower(country_code),'other')
    ;"

    echo $sql
    hive -e "$sql" >output/hb_nps_time_${beg_date}_${end_date}.txt
}

query_hb_nps_time 2021111400 2021111723 &
