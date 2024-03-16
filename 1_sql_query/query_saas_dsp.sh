#########################################################################
# File Name: query_saas_dsp.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 14 Dec 2021 01:35:57 PM CST
#########################################################################
#!/bin/bash
function query_daily_roi() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
	#if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code, 

    unit_ids='498064,1530173,1534519'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
	#country_code_condition="in('us', 'cn')"
	country_code_condition="is not null"
	dtm="substr(req_time,1,8)"

	sql="select $dtm as dtm, ad_type,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other') as country_code,
	is_hb,
	playable as is_makemoney_traffic,
	advertiser_id,
	platform,
	campaign_id,
	is_try_new,
    sum(total_req) as req, 
	sum(adx_impression) as adx_imp,
	sum(impression) as nor_imp,
	sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as click,
	sum(normal_install) as install,
	sum(adx_revenue) as adx_rev,
	sum(normal_revenue) as nor_rev,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
	sum(cost) as nor_cost,
	sum(adx_cost) as adx_cost,
    sum(cost)+sum(adx_cost) as total_cost
    from monitor_effect.model_effect_info_test 
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
    and req_time>='$begin_date' and req_time<='$end_date'
	and (bucket like '1%')
	and ad_type in('rewarded_video','interstitial_video')
	and unit_id $unit_id_condition
	and campaign_id!='371092647'
	and country_code $country_code_condition
	and playable in('Yes', 'No')
	group by $dtm, ad_type,
	platform,is_try_new,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other'),
	advertiser_id,
	campaign_id,
	playable,
	is_hb
	having sum(impression)+sum(adx_impression)>0;"

    echo "$sql"
    hive -e "$sql" >output/saas_dsp_effect_${begin_date}_${end_date}.txt
}

function query_sdp_fillrate_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_ids='473419,473421,473413,420402'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us','cn')"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    app_id,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
    split(ext_algo, ',')[0] as ext_cam,
	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
    if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(strategy, '\\;')[2] as is_emor,
    split(strategy, '\\;')[6] as makemoney_online,
    split(ext_algo, ',')[14] as tpl_group,
    split(ext_algo, ',')[15] as video_group,
    split(ext_algo, ',')[16] as edl_group,
    split(split(ext_algo, ',')[34], '\004')[0] image_sid,
    split(split(ext_algo, ',')[34], '\004')[1] video_sid,
    split(split(ext_algo, ',')[34], '\004')[2] playable_sid,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
    sum(split(split(ext_algo, ',')[26], '\004')[7]) as sum_mir_cpm,
    sum(split(split(ext_algo, ',')[26], '\004')[8]) as sum_emor_cpm,
    sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm_pre, 
    sum(split(split(ext_algo, ',')[25], '\004')[2]) as sum_fr_cpm_final, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    split(ext_algo, ',')[32] as region,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and (split(split(ext_algo, ',')[0], ':')[0] in(
	'374072652', '374158585', '374186238', '374358933', '374519991', '374562622', '374581624', '374647320', '374715929', '374726868', '374780371', '374780723', '374827851', '374932626', '374968532', '375001775', '375019122', '375019407', '375022962', '375120757', '375181186', '375256115', '375256939', '375263309', '375317617', '372577702','368414707','374080595','375544512','375631294','375292431','375609504','375784556','375658989','375406574','373921218','374295936','373776246','375582665','375457692','375744287','374983263','374932626','375537418','375544423','374987836','375695741','375485742','370822116','372574991','373078402','374245638','375723283','375001516'
	))
    and adtype in('rewarded_video','interstitial_video')
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, 
    split(ext_algo, ',')[0],
    split(split(ext_algo, ',')[34], '\004')[0],
    split(split(ext_algo, ',')[34], '\004')[1],
    split(split(ext_algo, ',')[34], '\004')[2],
    app_id,
	split(split(strategy, '\\;')[7], '-')[1],
    split(ext_algo, ',')[14],
    split(ext_algo, ',')[15],
    split(ext_algo, ',')[16],
    split(strategy, '\\;')[6],
    split(strategy, '\\;')[2],
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), 
    split(ext_algo, ',')[32],
    split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/sdp_fillrate_${beg_date}_${end_date}.txt
}

function query_sdp_rev_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_ids='473419,473421,473413,420402'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us','cn')"
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
    split(strategy, '\\;')[6] as makemoney_online,
	if(extra2!='', 'fill', 'no_fill') as if_fill, 
	split(ext_algo, ',')[37] as big_template,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	sum(split(ext_algo, ',')[3]) as sum_ivr,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
    sum(split(split(ext_algo, ',')[26], '\004')[7]) as sum_mir_cpm,
    sum(split(split(ext_algo, ',')[26], '\004')[8]) as sum_emor_cpm,
    sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm_pre, 
    sum(split(split(ext_algo, ',')[25], '\004')[2]) as sum_fr_cpm_final, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    split(ext_algo, ',')[32] as region,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and split(split(ext_algo, ',')[0], ':')[0] in('374072652', '374158585', '374186238', '374358933', '374519991', '374562622', '374581624', '374647320', '374715929', '374726868', '374780371', '374780723', '374827851', '374932626', '374968532', '375001775', '375019122', '375019407', '375022962', '375120757', '375181186', '375256115', '375256939', '375263309', '375317617', '372577702','368414707','374080595','375544512','375631294','375292431','375609504','375784556','375658989','375406574','373921218','374295936','373776246','375582665','375457692','375744287','374983263','374932626','375537418','375544423','374987836','375695741','375485742','370822116','372574991','373078402','374245638','375723283','375001516', '372065531','375694142','375509694','375317885','375833275','375522203','375760241','375829535','375406162','375179213','375019219','374047098','375658199','375798788','375120923','375565695','375921772','375361846','374877090','375872697','375059293','373276380','374068266','373026122','373835347','375603257','375779078','375711141','375738633','373883989','375436924','375376935','374221952','375339187','375870823','374389515','369058017','375388557','374841881','375111440','375912813','375457309','375722842','375850049','375619446','373076353','375799444')
    and ad_type in('rewarded_video','interstitial_video')
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, 
	split(ext_algo, ',')[37],
	split(split(strategy, '\\;')[7], '-')[1],
    split(strategy, '\\;')[6],
	get_json_object(ext_dsp,'$.is_hb'),
	if(extra2!='', 'fill', 'no_fill'),
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'),
	split(ext_algo, ',')[32]
    ;"

    echo "$sql"
    hive -e "$sql" >output/sdp_rev_${beg_date}_${end_date}.txt
}

#query_sdp_fillrate_by_time 2022010700 2022010723 &
#query_sdp_rev_by_time 2022011000 2022011423
query_daily_roi 2022031200 2022031423

