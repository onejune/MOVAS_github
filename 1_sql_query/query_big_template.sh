#########################################################################
# File Name: query_big_template.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Fri 03 Sep 2021 08:34:58 AM CST
#########################################################################
#!/bin/bash
function query_big_template_by_time() {
    beg_date=${1}
    end_date=${2}

	unit_ids='488099'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us','cn')"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
	split(split(strategy, '\\;')[7], '-')[1] as strategy,
	if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(ext_algo, ',')[36] as btl_cnt,
	split(ext_algo, ',')[37] as big_template,
    get_json_object(ext_dsp,'$.is_hb') as is_hb,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
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
	and adtype in('rewarded_video','interstitial_video')
	and strategy like '%MNormalAlpha%'
    group by concat(yyyy,mm,dd), ad_type, 
	split(split(strategy, '\\;')[7], '-')[1],
	get_json_object(ext_dsp,'$.is_hb'),
    split(ext_algo, ',')[36],
	split(ext_algo, ',')[37],
	if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), 
	split(ext_algo, ',')[40]
	;"

    echo $sql
    hive -e "$sql" >output/big_template_cnt_${beg_date}_${end_date}.txt
}

query_big_template_by_time 2022012100 2022012123
