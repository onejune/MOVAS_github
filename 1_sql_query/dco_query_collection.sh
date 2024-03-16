#########################################################################
# File Name: dco_query_collection.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Fri 14 Aug 2020 12:15:57 PM CST
#########################################################################
#!/bin/bash

function query_dco_creative_req() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
    unit_id_condition="in('293668','280124')"
    unit_id_condition="is not null"
    country_code_condition="in('us')"
    udf_jar="s3://mob-emr-test/wanjun/udf/udf_creative_parser.jar"

    creative_field="split(get_crt_str(ext_stats), '#')[2]"

    sql="
    add jar $udf_jar;
    CREATE TEMPORARY FUNCTION get_crt_str AS 'com.mobvista.udf.creative.GetCrtStr';
    select substr(split(ext_algo, ',')[1],1,8) as dtm,
    ad_type, unit_id, lower(country_code) as cc,
	campaign_id,	
    split($creative_field, ';')[0] as creative_type,
    split($creative_field, ';')[2] as did,
    split($creative_field, ';')[5] as uniq_cid,
    count(*) as req
    from dwh.ods_adn_trackingnew_impression_merge 
    where concat(yyyy,mm,dd,hh)>='$begin_date'
    and concat(yyyy,mm,dd,hh)<='$end_date'
    and strategy like '%MNormalAlphaModelRankerHH%' 
    and ad_type in('rewarded_video', 'interstitial_video')
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by substr(split(ext_algo, ',')[1],1,8), 
    ad_type, unit_id, lower(country_code),campaign_id,
	split($creative_field, ';')[0],
	split($creative_field, ';')[2],
	split($creative_field, ';')[5]
	;
    "
    echo $sql
    hive -e "$sql" >output/dco_crt_request_${begin_date}_${end_date}.txt
}

function query_dco_creative_effect() {
    beg_date=$1
    end_date=$2

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    #unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    country_code_condition="in('us','cn')"
    publisher_ids='10714'
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition="is not null"

    sql="select 
    concat(year,month,day) as dtm,
    ad_type, big_template_id, app_id, 
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as country_code,
	playable, playable_id,
    if(playable='none', 'none', if(playable_did='0', 'no', 'yes')) as is_playable_dco,
    sum(impression) as imp,
    sum(click) as clk, sum(install) as ins,
    sum(revenue) as rev
    from  monitor_effect.creative_info_table 
    where concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date 
    and ad_type in('rewarded_video','interstitial_video', 'sdk_banner') 
    and unit_id $unit_id_condition
    and (bucket_id like '1_cnrr2%' or bucket_id like '1%')
    and lower(country_code) $country_code_condition
    and publisher_id $publisher_id_condition
    group by 
    concat(year,month,day),
	playable, playable_id,
    ad_type, big_template_id, app_id,
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'),
    if(playable='none', 'none', if(playable_did='0', 'no', 'yes'))
    ;"

    echo $sql
    hive -e "$sql" >output/dco_creative_effect_${beg_date}_${end_date}.txt

}

#query_dco_creative_req 2021012500 2021012512i
query_dco_creative_effect 2021040100 2021040123
