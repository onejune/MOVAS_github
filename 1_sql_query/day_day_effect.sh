#########################################################################
# File Name: day_day_effect.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Fri 19 Jun 2020 04:32:51 PM CST
#########################################################################
#!/bin/bash
function query_hb_day_day_effect() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:beg_date or end_date is invalid."
        exit
    fi

    sql="select concat(year,month,day) as dtm, unit_id, campaign_id, ad_type, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code,
    platform, bucket, is_hb,
    sum(total_req) as req, sum(impression) as normal_imp, sum(adx_impression) as adx_imp,
    sum(normal_install) as install, sum(normal_revenue) as normal_rev, sum(adx_revenue) as adx_rev,
    sum(cost) as normal_cost, sum(adx_cost) as adx_cost
    from monitor_effect.model_effect_info_test
    where
    ((concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date) 
    or (concat(year,month,day,hour)>=$beg_date1 and concat(year,month,day,hour)<=$end_date1)
    or (concat(year,month,day,hour)>=$beg_date2 and concat(year,month,day,hour)<=$end_date2)) 
	and(bucket like '3%' or bucket like '1%')
	and ad_type='sdk_banner'
	and country_code='us'
    group by concat(year,month,day), ad_type, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'),
    platform, bucket, is_hb, unit_id, campaign_id;"

    echo $sql
    hive -e "$sql" >output/hb_day_day_effect_${beg_date}_${end_date}.txt
}

function query_day_day_roi() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:beg_date or end_date is invalid."
        exit
    fi

	sql="select concat(year,month,day) as dtm, substr(req_time, 1, 8) as req_time, publisher_id, is_try_new, 
	platform,
	advertiser_id,
	ad_type, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, is_hb,
    sum(total_req) as req, 
    sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as total_clk,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
    sum(cost)+sum(adx_cost) as total_cost,
    (sum(adx_revenue)+sum(normal_revenue))-(sum(cost)+sum(adx_cost)) as margin,
    (sum(impression)+sum(adx_impression))/(sum(total_req)) as imp_rate
    from monitor_effect.model_effect_info_test
    where
    ((concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date) 
    or (concat(year,month,day,hour)>=$beg_date1 and concat(year,month,day,hour)<=$end_date1)
    or (concat(year,month,day,hour)>=$beg_date2 and concat(year,month,day,hour)<=$end_date2)) 
	and (bucket like '1%')
    and ad_type in('interstitial_video','rewarded_video')
	and country_code in('us')
	group by concat(year,month,day), substr(req_time, 1, 8),
	publisher_id, is_try_new, ad_type, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'),
	advertiser_id,
    is_hb, 
	platform
    ;"

    echo $sql
    hive -e "$sql" >output/day_day_roi2_${beg_date}_${end_date}.txt
}

function query_day_day_roi_campaign() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
	
	unit_id_condition="in(483792,209912,433756,439274,343784,512355,474230,448418)"
    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:beg_date or end_date is invalid."
        exit
    fi

	sql="select concat(year,month,day) as dtm, substr(req_time, 1, 8) as req_time, 
	campaign_id, publisher_id, is_try_new, unit_id, ad_type, 
	if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, is_hb,
    sum(total_req) as req, 
    sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as total_clk,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
    sum(cost)+sum(adx_cost) as total_cost,
    (sum(adx_revenue)+sum(normal_revenue))-(sum(cost)+sum(adx_cost)) as margin,
    (sum(impression)+sum(adx_impression))/(sum(total_req)) as imp_rate
    from monitor_effect.model_effect_info_test
    where
    ((concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date) 
    or (concat(year,month,day,hour)>=$beg_date1 and concat(year,month,day,hour)<=$end_date1)
    or (concat(year,month,day,hour)>=$beg_date2 and concat(year,month,day,hour)<=$end_date2)) 
	and (bucket like '1%')
    and ad_type in('interstitial_video','rewarded_video')
    and country_code in('us', 'cn')
	and unit_id $unit_id_condition
	group by concat(year,month,day), substr(req_time, 1, 8),
	publisher_id, is_try_new, ad_type, if(lower(country_code) in('cn','us'),lower(country_code),'other'),
    is_hb, unit_id, campaign_id
    ;"

    echo $sql
    hive -e "$sql" >output/day_day_roi_${beg_date}_${end_date}.txt
}

function query_day_day_copc() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids='435266, 429003, 389993'
	publisher_ids='10714'
    output="output/${beg_date}_${end_date}_day_day_copc.txt"
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('us')"
	publisher_id_condition="in($publisher_ids)"
	publisher_id_condition="is not null"

    imp_sql="select ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, unit_id, lower(country_code) as country_code, publisher_id, app_id,
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name, platform,
    split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
	split(strategy, '\\;')[4] as crt_trynew,
	campaign_id,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	split(ext_algo, ',')[2] as price,
	split(ext_algo, ',')[8] as trynew_type,
	split(ext_algo, ',')[3] as cr_ivr,
	split(ext_algo, ',')[7] as ecpm_floor,
	split(split(ext_algo, ',')[24], '\004')[0] as imp_control_rate
    from dwh.ods_adn_trackingnew_impression_merge
	where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2))
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created, campaign_id,
    split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install_merge
    where
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2))
    and ad_type in('rewarded_video','interstitial_video')
    "

    sql="select dtm, placement_id, publisher_id, app_id, 
	country_code, ad_type, platform, is_hb, trynew_type,crt_trynew,
    count(requestid) as imp, sum(max_fr) as max_fr, sum(rev) as rev,
	sum(imp_control_rate) as sum_imp_ctrl_rate,
	sum(price) as price, sum(cr_ivr) as cr_ivr, sum(ecpm_floor) as ecpm_floor
    from (select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id, a.app_id as app_id,
	a.dtm as dtm, a.ad_type as ad_type, a.unit_id as unit_id, 
	a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
	a.campaign_id as campaign_id,
	a.crt_trynew as crt_trynew,
    b.rev as rev, a.is_hb as is_hb,
	a.imp_control_rate as imp_control_rate,
	a.price as price, a.cr_ivr as cr_ivr, a.trynew_type as trynew_type, a.ecpm_floor as ecpm_floor
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=24*3600)c
    group by dtm, placement_id, ad_type, publisher_id, app_id, 
	country_code, platform, is_hb, trynew_type, crt_trynew
	;"

    #sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
    #from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

    echo "sql: $sql"
    hive -e "$sql" >$output
}

function query_day_day_fillrate() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="461652,461649,1547633,461630"
	app_id_condition="is not null"
	#app_id_condition="in('147702','132737','126154','127773', '132737', '127773', '126154', '124927', '120516')"
	#unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('us')"
	#country_code_condition="is not null"
	
	#split(ext_algo, ',')[0] as campaign,
	#split(ext_algo, ',')[37] as big_template,
    #split(ext_algo, ',')[14] as tpl_group,
    #split(ext_algo, ',')[15] as video_tpl,
    #split(ext_algo, ',')[16] as endcard_tpl, 

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
	platform,
	app_id,
	split(strategy, ';')[2] as recalled_cnt,
	split(strategy, '\\;')[4] as cam_trynew_type,
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, if(extra2!='', 'fill', 'no_fill') as if_fill,
	split(ext_algo, ',')[37] as big_template,
	split(ext_algo, ',')[15] as video_tpl,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[26], '\004')[4]) as sum_cnrr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
	sum(split(split(ext_algo, ',')[26], '\004')[7]) as sum_mir_cpm,
	sum(split(split(ext_algo, ',')[26], '\004')[8]) as sum_emor_cpm,
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate,
	split(ext_algo, ',')[36] as btl_cnt,
	if(split(split(ext_algo, ',')[26], '\004')[4]=0, 0, 1) as cnrr_valid,
	sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and strategy like '%MNormalAlpha%'
    and unit_id $unit_id_condition
	and app_id $app_id_condition
	and ad_type in('rewarded_video','interstitial_video')
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type,
	split(strategy, ';')[2],
	platform,
	app_id,
	split(ext_algo, ',')[15],
	if(split(split(ext_algo, ',')[26], '\004')[4]=0, 0, 1),
	split(ext_algo, ',')[37] ,
	split(strategy, '\\;')[4],
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
	split(ext_algo, ',')[36]
	;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_fillrate_${beg_date}_${end_date}.txt
}

function query_day_day_fillrate_hourly() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="281851,384367,270592"
	app_id_condition="is not null"
	#unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('us')"
	#country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd,hh) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
	platform,unit_id,
	app_id,
	if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
	split(strategy, '\\;')[3] as crt_trynew_type,
	split(strategy, '\\;')[4] as cam_trynew_type,
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate,
	split(ext_algo, ',')[36] as btl_cnt,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge 
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and ext_reducefillreq=1
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd,hh), ad_type,
	platform,
	app_id,
	unit_id,
	split(strategy, '\\;')[3], split(strategy, '\\;')[4],
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
	split(ext_algo, ',')[36],
	if(idfa='00000000-0000-0000-0000-000000000000',0,1),
	split(ext_algo, ',')[14],
	split(ext_algo, ',')[15]
	;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_fillrate_hourly_${beg_date}_${end_date}.txt
}

function query_day_day_fillrate_campaign() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="483792,209912,433756,439274,343784,512355,474230,448418"
	app_ids="145734"
	app_id_condition="in($app_ids)"
	app_id_condition="is not null"
	#unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, ext_placementId, split(ext_algo, ',')[40] as if_lower_imp, 
	app_id, unit_id,
	split(ext_algo, ',')[0] as campaign,
	split(ext_algo, ',')[14] as tpl_group,
    split(ext_algo, ',')[15] as video_tpl,
    split(ext_algo, ',')[16] as end_tpl,
    split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
    split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
    split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge 
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, ext_placementId, app_id, unit_id, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	split(ext_algo, ',')[0],
	split(ext_algo, ',')[14],
    split(ext_algo, ',')[15],
    split(ext_algo, ',')[16],
    split(split(ext_algo, ',')[20], '\004')[0],
    split(split(ext_algo, ',')[20], '\004')[1],
    split(split(ext_algo, ',')[20], '\004')[2],
	if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
	split(ext_algo, ',')[14],
	split(ext_algo, ',')[15]
	;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_fillrate_campaign_${beg_date}_${end_date}.txt
}
function query_day_day_creative_cpm() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:beg_date or end_date is invalid."
        exit
    fi

    unit_ids='384365'
    #unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    country_code_condition="in('us')"
    publisher_ids='10714'
    publisher_id_condition="in($publisher_ids)"
    publisher_id_condition="is not null"

    sql="select concat(year,month,day) as dtm, 
    publisher_id, ad_type, 
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    big_template_id, app_id,
    templategroup, videotemplate,
    sum(impression) as imp,
    sum(click) as clk, sum(install) as ins,
    sum(revenue) as rev
    from  monitor_effect.creative_info_table 
    where ((concat(year,month,day,hour) between $beg_date1 and $end_date1)
    or (concat(year,month,day,hour) between $beg_date2 and $end_date2)
    or (concat(year,month,day,hour) between $beg_date and $end_date))
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
    and (bucket_id like '1_cnrr2%' or bucket_id like '1%')
    and lower(country_code) $country_code_condition
    and publisher_id $publisher_id_condition
    group by 
	concat(year,month,day),
	publisher_id, ad_type, 
    if(lower(country_code) in('cn','us'),lower(country_code),'other'),
    big_template_id, app_id,
    templategroup, videotemplate
    ;"

    echo $sql
    hive -e "$sql" >output/day_day_creative_effect_${beg_date}_${end_date}.txt
}                                             




function query_HB_day_day_req_campaign() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="483792,209912,433756,439274,343784,512355,474230,448418"
    app_ids="145734"
    app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    #unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, ext_placementId, split(ext_algo, ',')[40] as if_lower_imp, 
    app_id, unit_id,
    split(ext_algo, ',')[0] as campaign,
    split(ext_algo, ',')[14] as tpl_group,
    split(ext_algo, ',')[15] as video_tpl,
    split(ext_algo, ',')[16] as end_tpl,
    split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
    split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
    split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request 
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and unit_id $unit_id_condition
	 and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, ext_placementId, app_id, unit_id, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[0],
    split(ext_algo, ',')[14],
    split(ext_algo, ',')[15],
    split(ext_algo, ',')[16],
    split(split(ext_algo, ',')[20], '\004')[0],
    split(split(ext_algo, ',')[20], '\004')[1],
    split(split(ext_algo, ',')[20], '\004')[2],
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_hb_req_campaign_${beg_date}_${end_date}.txt
}


function query_HB_day_day_req() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="435259,512355,498963,312718"
    app_ids="145734"
    app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    #unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, ext_placementId, split(ext_algo, ',')[40] as if_lower_imp, 
    app_id, unit_id,
	split(ext_algo, ',')[0] as algo_cam,
	split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
    split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
    split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request 
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, ext_placementId, app_id, unit_id, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	split(ext_algo, ',')[0],
    split(split(ext_algo, ',')[20], '\004')[0],
    split(split(ext_algo, ',')[20], '\004')[1],
    split(split(ext_algo, ',')[20], '\004')[2],
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_hb_req_${beg_date}_${end_date}.txt
}

function query_HB_day_day_req_hourly() {
    beg_date=${1}
    end_date=${2}
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    unit_ids="483792,209912,433756,439274,343784,512355,474230,448418"
    app_ids="145734"
    app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    #unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd,hh) as dtm, ad_type, ext_placementId, split(ext_algo, ',')[40] as if_lower_imp, 
    app_id, unit_id,
    split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
    split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
    split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request 
    where 
    ((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
    or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2)) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd,hh), ad_type, ext_placementId, app_id, unit_id, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
    split(split(ext_algo, ',')[20], '\004')[0],
    split(split(ext_algo, ',')[20], '\004')[1],
    split(split(ext_algo, ',')[20], '\004')[2],
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/day_and_days_hb_req_hourly_${beg_date}_${end_date}.txt
}

#query_HB_day_day_req_campaign 2021081000 2021081023 &
#query_HB_day_day_req 2021081200 2021081212 &
#query_HB_day_day_req_hourly 2021081200 2021081223 &

#query_day_day_creative_cpm 2021032500 2021032509
#query_day_day_roi 2022030200 2022030208 &
#query_day_day_roi_campaign 2021081000 2021081010 &
#query_day_day_copc 2021081000 2021081008 &
query_day_day_fillrate 2022030200 2022030215 &
#query_day_day_fillrate_hourly 2021081200 2021081223 &
