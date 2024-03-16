function query_wf_nps_time() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition="is not null"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    platform, 
    split(split(ext_algo, ',')[45], '\004')[0] as region,
    split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
    sum(split(split(ext_algo, ',')[33], '\004')[0]) as sum_time_use,
    sum(split(split(ext_algo, ',')[33], '\004')[1]) as sum_task_cnt,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, 
    split(split(strategy, '\\;')[7], '-')[1],
    platform,
    split(split(ext_algo, ',')[45], '\004')[0],
    if(lower(country_code) in('cn','us'),lower(country_code),'other')
    ;"

    echo $sql
    hive -e "$sql" >output/wf_nps_time_${beg_date}_${end_date}.txt
}

function query_hb_nps_time() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition="is not null"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type,
    if(lower(country_code) in('cn','us', 'in'),lower(country_code),'other') as country_code, 
    platform, 
    split(split(ext_algo, ',')[45], '\004')[0] as region,
    split(split(strategy, '\\;')[7], '-')[3] as strategy_name,
    sum(split(split(ext_algo, ',')[33], '\004')[0]) as sum_time_use,
    sum(split(split(ext_algo, ',')[33], '\004')[1]) as sum_task_cnt,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, 
    split(split(strategy, '\\;')[7], '-')[3],
    platform,
    split(split(ext_algo, ',')[45], '\004')[0],
    if(lower(country_code) in('cn','us', 'in'),lower(country_code),'other')
    ;"

    echo $sql
    hive -e "$sql" >output/hb_nps_time_${beg_date}_${end_date}.txt
}

#query_wf_nps_time 2021112215 2021112323 &
query_hb_nps_time 2021121300 2021121923 &



