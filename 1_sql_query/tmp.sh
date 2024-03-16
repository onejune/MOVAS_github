#########################################################################
# File Name: tmp.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Mon 07 Mar 2022 07:38:00 PM CST
#########################################################################
#!/bin/bash
#########################################################################
#global variable
unit_ids='201096,198010,206663,198008,175874,187113,198608,175870,221535,165378,206663,198608,227083'
algo_field="
	sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(ext_algo, ',')[23] as flow_type,
    split(ext_algo, ',')[8] as trynew_type,
	split(ext_algo, ',')[36] as if_support_polaris"


#########################################################################

root_dir="s3://mob-emr-test/wanjun/hive_data/hive_query_collection"
hive_out="$root_dir/hive_out/"

function query_daily_roi() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
	#if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code, 
	#and ad_type in('rewarded_video','interstitial_video', 'sdk_banner', 'more_offer', 'native')

    unit_ids='498064,1530173,1534519'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
	country_code_condition="in('us')"
	#country_code_condition="is not null"
	
	sql="select substr(req_time,1,8) as dtm, 
	ad_type,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other') as country_code,
	is_hb,
    bucket,
	playable as is_makemoney_traffic,
    advertiser_id,
	platform,
	campaign_id,
    sum(total_req) as req, 
	sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as click,
	sum(normal_install) as install,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
    sum(cost)+sum(adx_cost) as total_cost
    from monitor_effect.model_effect_info_test 
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
	and req_time>='$begin_date' and req_time<='$end_date'
	and (bucket like '1%')
	and unit_id $unit_id_condition
	and campaign_id!='371092647'
    and ad_type in('rewarded_video','interstitial_video')
	and country_code $country_code_condition
	group by substr(req_time,1,8), 
	ad_type,
	platform,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other'),
	bucket,
    advertiser_id,
	campaign_id,
	playable,
	is_hb
	;"

    echo "$sql"
    output="days_effect_${begin_date}_${end_date}_layer.txt"
    python spark-sql-submit.py --sql "$sql" --output_partition_num 1 --local_file "$output" --s3_path ""
    aws s3 cp $output $hive_out
}

function query_effect_from_tracking_with_campaign() {
    beg_date=${1}
    end_date=${2}

    unit_ids='461649'
    publisher_ids='10714'
    output="output/${beg_date}_${end_date}_effect_from_tracking.txt"
    unit_id_condition='is not null'
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select 
    1 as imp,
    platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, app_id,
	unit_id, campaign_id,
    ext_endcard, ext_rv_template,
	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
    split(ext_algo, ',')[3] as cr_ivr,
    split(ext_algo, ',')[4] as cr_cpm,
	split(split(ext_algo, ',')[25], '\004')[1] as fr_cpm,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	split(ext_algo, ',')[8] as trynew_type,
	split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(split(ext_algo, ',')[20], '\004')[1] as video_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] as playable_trynew,
	split(ext_algo, ',')[2] as price
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    clk_sql="select requestid,
	1 as clk
    from dwh.ods_adn_trackingnew_click_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video')
    "

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video')
    "

    sql="select dtm, placement_id, ad_type, publisher_id, app_id, country_code, platform, is_hb, trynew_type,
	tpl_trynew,playable_trynew,video_trynew,
    ext_endcard, ext_rv_template,
	tpl_group,video_tpl,
	unit_id, campaign_id,
	sum(cr_cpm) as cr_cpm, sum(fr_cpm) as fr_cpm, sum(cr_ivr) as cr_ivr, 
    sum(imp) as imp,
    sum(clk) as clk,
    sum(rev) as rev, 
    sum(ins) as ins
    from (
        select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id, a.app_id as app_id,
	    a.dtm as dtm, a.ad_type as ad_type, a.price as price, a.trynew_type as trynew_type,
        a.ext_endcard as ext_endcard, a.ext_rv_template as ext_rv_template,
	    a.tpl_group as tpl_group, a.video_tpl as video_tpl,
	    a.tpl_trynew as tpl_trynew,
	    a.video_trynew as video_trynew,
	    a.unit_id as unit_id, a.campaign_id as campaign_id,
	    a.playable_trynew as playable_trynew,
        a.is_hb as is_hb,
	    a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.cr_cpm as cr_cpm, a.cr_ivr as cr_ivr, a.fr_cpm as fr_cpm,
        a.imp as imp,b.rev as rev, b.ins as ins, c.clk as clk
        from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid left join ($clk_sql)c on a.requestid=c.requestid
    )
    group by dtm, placement_id, ad_type, publisher_id, app_id, country_code, platform, is_hb, trynew_type,
	tpl_trynew,playable_trynew,video_trynew,
    ext_endcard, ext_rv_template,
	video_tpl,tpl_group,
	unit_id, campaign_id
	having imp>0;"

    echo "sql:$sql"
    hive -e "$sql" >$output
    #python spark-sql-submit.py --sql "$sql" --output_partition_num 1 --local_file "$output" --s3_path ""
    #aws s3 cp $output $hive_out
}


query_effect_from_tracking_with_campaign 2022030400 2022030823
#query_daily_roi 2022030400 2022030823




