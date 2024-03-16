#########################################################################
# File Name: query_cnrr.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Wed 23 Dec 2020 07:02:04 PM CST
#########################################################################
#!/bin/bash
function query_effect_from_tracking() {
    beg_date=${1}
    end_date=${2}
    record_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$record_time=======query for $beg_date - $end_date========"
    output=output/${beg_date}_${end_date}_effect_from_tracking.txt
    echo $record_time

    ins_delay_hours=24
    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('us','cn')"

    imp_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, split(split(strategy, '\\;')[7], '-')[2] as strategy_name, 
    ad_type, if(lower(country_code) in('us','cn', 'jp', 'uk'),lower(country_code),'other') as cc,
    split(ext_algo, ',')[8] as trynew_type, 
    split(ext_algo, ',')[23] as flow_type,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    split(split(ext_algo, ',')[30], '\004')[1] as cnrr_rank,
    split(split(ext_algo, ',')[30], '\004')[0] as recall_rank,
    count(*) as imp,
	sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr,
	sum(split(split(ext_algo, ',')[26], '\004')[4]) as sum_cnrr_ivr
    from dwh.ods_adn_trackingnew_impression_merge 
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and strategy like 'MNormalAlpha%' 
    and adType in('rewarded_video','interstitial_video') 
    and split(ext_algo, ',')[1]>=$beg_date 
    and split(ext_algo, ',')[1]<=$end_date 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
	and split(ext_algo, ',')[24]='1'
    group by substr(split(ext_algo, ',')[1],1,8), ad_type,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp', 'uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[8],split(ext_algo, ',')[23],
	split(split(ext_algo, ',')[30], '\004')[1],
	split(split(ext_algo, ',')[30], '\004')[0],
    split(split(strategy, '\\;')[7], '-')[2]"

    ins_end_date=$(date -d "+$ins_delay_hours hour ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    echo "ins_end_date:$ins_end_date"

    ins_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, split(split(strategy, '\\;')[7], '-')[2] as strategy_name, 
    ad_type, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc,
    split(ext_algo, ',')[8] as trynew_type,
    split(ext_algo, ',')[23] as flow_type,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    split(split(ext_algo, ',')[30], '\004')[1] as cnrr_rank,
	split(split(ext_algo, ',')[30], '\004')[0] as recall_rank,
    count(*) as ins, sum(split(ext_algo, ',')[2]) as rev 
    from dwh.ods_adn_trackingnew_install_merge 
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${ins_end_date} 
    and strategy like 'MNormalAlpha%' and adType in('rewarded_video','interstitial_video', 'sdk_banner') 
    and split(ext_algo, ',')[1]>=$beg_date and split(ext_algo, ',')[1]<=$end_date
    group by substr(split(ext_algo, ',')[1],1,8), ad_type,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
	split(ext_algo, ',')[8],split(ext_algo, ',')[23],
	split(split(ext_algo, ',')[30], '\004')[1],
	split(split(ext_algo, ',')[30], '\004')[0],
    split(split(strategy, '\\;')[7], '-')[2]"

    sql="select a.*, b.ins, b.rev 
    from($imp_sql) a left join($ins_sql) b 
    on a.dtm=b.dtm 
    and a.strategy_name=b.strategy_name 
    and a.ad_type=b.ad_type and a.cc=b.cc and a.trynew_type=b.trynew_type and a.flow_type=b.flow_type
    and a.cnrr_rank=b.cnrr_rank
	and a.recall_rank=b.recall_rank
    and a.idfa=b.idfa;"

    echo "sql:$sql"

    hive -e "$sql" >$output
}

query_effect_from_tracking 2021110200 2021110323
