#########################################################################
# File Name: polaris_query.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 03 Nov 2020 12:06:41 PM CST
#########################################################################
#!/bin/bash

function query_polaris_effect_from_tracking() {
    beg_date=${1}
    end_date=${2}
    record_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$record_time=======query for $beg_date - $end_date========"
    output=output/${beg_date}_${end_date}_polaris_effect_from_tracking.txt
    echo $record_time

    ins_delay_hours=24
    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('us','cn','jp','uk')"

    imp_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, split(split(strategy, '\\;')[7], '-')[1] as strategy_name, 
    ad_type, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc,
    split(ext_algo, ',')[8] as trynew_type, 
    split(ext_algo, ',')[14] as tpl_group, 
    split(ext_algo, ',')[15] as video_tpl, 
    split(ext_algo, ',')[23] as flow_type,
    split(ext_algo, ',')[36] as polaris,
	split(ext_algo, ',')[37] as big_tem,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and strategy like 'MNormalAlpha%' 
    and ad_type in('rewarded_video','interstitial_video', 'sdk_banner','more_offer') 
    and split(ext_algo, ',')[1]>=$beg_date 
    and split(ext_algo, ',')[1]<=$end_date 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by substr(split(ext_algo, ',')[1],1,8), ad_type,
    split(ext_algo, ',')[36],
    split(ext_algo, ',')[14],split(ext_algo, ',')[15],
	split(ext_algo, ',')[37],
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[8],split(ext_algo, ',')[23],
    split(split(strategy, '\\;')[7], '-')[1]"

    ins_end_date=$(date -d "+$ins_delay_hours hour ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    echo "ins_end_date:$ins_end_date"

    ins_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, split(split(strategy, '\\;')[7], '-')[1] as strategy_name, 
    ad_type, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc, 
    split(ext_algo, ',')[8] as trynew_type,
    split(ext_algo, ',')[23] as flow_type,
    split(ext_algo, ',')[36] as polaris,
    split(ext_algo, ',')[14] as tpl_group, 
    split(ext_algo, ',')[15] as video_tpl, 
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
	split(ext_algo, ',')[37] as big_tem,
    count(*) as ins, sum(split(ext_algo, ',')[2]) as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${ins_end_date} 
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video', 'sdk_banner', 'more_offer') 
    and split(ext_algo, ',')[1]>=$beg_date and split(ext_algo, ',')[1]<=$end_date
    and (unix_timestamp(concat(yyyy,mm,dd,hh), 'yyyyMMddHH')-unix_timestamp(split(ext_algo, ',')[1], 'yyyyMMddHH'))/3600<=$ins_delay_hours  
    group by substr(split(ext_algo, ',')[1],1,8), ad_type,
    split(ext_algo, ',')[36],
    split(ext_algo, ',')[14], split(ext_algo, ',')[15],
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[8],split(ext_algo, ',')[23],
	split(ext_algo, ',')[37],
    split(split(strategy, '\\;')[7], '-')[1]"

    sql="select a.*, b.ins, b.rev 
    from($imp_sql) a left join($ins_sql) b 
    on a.dtm=b.dtm 
    and a.strategy_name=b.strategy_name 
    and a.ad_type=b.ad_type and a.cc=b.cc and a.trynew_type=b.trynew_type and a.flow_type=b.flow_type
    and a.polaris=b.polaris
    and a.tpl_group=b.tpl_group
	and a.big_tem=b.big_tem
    and a.video_tpl=b.video_tpl
    and a.idfa=b.idfa;"

    echo "sql:$sql"

    #hive -e "$sql" >$output

	sh hive_export_v2.sh "$sql" "s3://mob-emr-test/wanjun/ts/text_spark.txt"
}


query_polaris_effect_from_tracking 2022021100 2022021101
