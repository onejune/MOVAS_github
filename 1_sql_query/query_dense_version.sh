function query_creative_by_time() {
    beg_date=${1}
    end_date=${2}

	unit_ids='281851'
	unit_id_condition="in ($unit_ids)"
    country_code_condition="in('us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
	extra2,split(ext_algo, ',')[0] as algo_cam,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, platform, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(split(ext_algo, ',')[34], '\004')[1] as video_crt, 
    split(split(ext_algo, ',')[34], '\004')[2] as playable_crt, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm_pre, 
    sum(split(split(ext_algo, ',')[25], '\004')[2]) as sum_fr_cpm_final, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
	sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video','sdk_banner')
    group by concat(yyyy,mm,dd), ad_type, unit_id,platform,if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    split(split(ext_algo, ',')[34], '\004')[1], 
    split(split(ext_algo, ',')[34], '\004')[2], 
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
	extra2,split(ext_algo, ',')[0]
	;"

    echo $sql
    hive -e "$sql" >output/creative_impression_${beg_date}_${end_date}.txt
}


function query_fillrate_by_time() {
    beg_date=${1}
    end_date=${2}

    app_ids='135782,131585,136458'
	unit_ids='161622'

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    split(ext_algo, ',')[39] as bid_floor,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, platform, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(split(strategy, '\\;')[7], '-')[0] as strategy_name, 
	split(ext_algo, ',')[23] as flow_type,
	split(ext_algo, ',')[36] as polaris,
	sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm_pre, 
    sum(split(split(ext_algo, ',')[25], '\004')[2]) as sum_fr_cpm_final, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
	sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration,
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id in($unit_ids)
    group by concat(yyyy,mm,dd), ad_type, unit_id,platform,if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
    split(ext_algo, ',')[39],
	split(ext_algo, ',')[23],
	split(ext_algo, ',')[36],
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    split(split(strategy, '\\;')[7], '-')[0];"

    echo $sql
    hive -e "$sql" >output/fillrate_${beg_date}_${end_date}.txt
}

function query_algo_by_time() {
    beg_date=${1}
    end_date=${2}

    app_ids='135782,131585,136458'
	unit_ids='161622'

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    split(ext_algo, ',')[39] as bid_floor,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, platform, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(split(strategy, '\\;')[7], '-')[0] as strategy_name, 
	split(ext_algo, ',')[23] as flow_type,
	split(ext_algo, ',')[36] as polaris,
	ext_dsp,
    ext_algo
	from dwh.ods_adn_trackingnew_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id in($unit_ids)
	and ext_dsp 
	;"
    echo $sql
    hive -e "$sql" >output/fillrate_${beg_date}_${end_date}.txt
}

function query_dsp_algo_by_time() {
    beg_date=${1}
    end_date=${2}

    unitids="161622"
    sql="SELECT concat(yyyy,mm,dd) AS datestr,
            device_info.os AS platform, 
            CASE 
                     WHEN imp_info[0].at=13 THEN 'online_video' 
                     WHEN imp_info[0].at=15 THEN 'native'
            END AS ad_type, 
            CASE 
                     WHEN t2.adx_dsp.id=6 THEN 'asdsp' 
                     WHEN t2.adx_dsp.id=13 THEN 'pioneer' 
            END      AS dsp, 
            imp_info[0].tid AS unit_id,
            ext_algo
    FROM     dwh.ods_adn_adx_v1_request AS t1 lateral VIEW explode(dsp_response) t2 AS adx_dsp
    WHERE    concat(yyyy,mm,dd) >= $beg_date and concat(yyyy,mm,dd) < $end_date
    AND      imp_info[0].ishb is null 
    AND      imp_info[0].at IN (13, 15) 
    AND      t2.adx_dsp.id  IN (6,13) 
    AND      imp_info[0].tid IN ($unitids)
    limit 1000000;"
    echo $sql
    hive -e "$sql" >output/fillrate_${beg_date}_${end_date}.txt
}

function query_model_version_time() {
    beg_date=${1}
    end_date=${2}

    sql="select 
    concat(yyyy,mm,dd) as day, 
    concat(yyyy,mm,dd, hh) as dtm,
	unit_id,
	if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name,
	split(ext_algo, ',')[47] as dense_version,
    count(*) as req
    from dwh.ods_adn_trackingnew_request
    where 
    concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date
	and strategy like 'MNormalAlpha%' 
	and ad_type in('rewarded_video','interstitial_video','sdk_banner')
    group by
    concat(yyyy,mm,dd),
    concat(yyyy,mm,dd, hh),
	split(split(strategy, '\\;')[7], '-')[0],
	unit_id,
	if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    split(ext_algo, ',')[47]
    ;"

    echo $sql
    hive -e "$sql" >output/model_version_${beg_date}_${end_date}.txt
}

query_model_version_time 2021042718 2021042723

#query_dsp_algo_by_time 2021030500 2021030700

#query_fillrate_by_time 2021030400 2021031023 
#query_algo_by_time 2021030400 2021031023 
#query_creative_by_time 2020082400 2020090200
