#########################################################################
# File Name: query_dev_id.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sat 24 Apr 2021 11:11:50 AM CST
#########################################################################
#!/bin/bash

function query_imp() {
    beg_date=${1}
    end_date=${2}

	unit_id_condition="is not null"
    country_code_condition="is not null"

    sql="select
	concat(yyyy,mm,dd) as dtm,
	ad_type,
	country_code,
    app_id,
	ext_finalpackagename,ext_campaignpackagename,
	ext_algo, ext_stats, ext_stats2,
	ext_polarisCreativeData,
	ext_polarisTplData,
	if(extra2!='', 'fill', 'no_fill') as if_fill,
	extra2,
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name
    from dwh.ods_adn_trackingnew_impression
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video')
	and campaign_id in('374562622')
	;"

    echo $sql
    hive -e "$sql" >output/saas_dsp_detailed_${beg_date}_${end_date}.txt
}

query_imp 2021120700 2021120923
