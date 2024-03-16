appids='134648'
unit_ids='205277,225469,230306'
campaigns='349599464,349754066,316207488'

function query_waterfall_user_imp_req() {
    beg_date=${1}
    end_date=${2}
    record_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$record_time=======query for $beg_date - $end_date========"
    output=output/user_req_imp_${beg_date}_${end_date}.txt
    echo $record_time

    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    #appid_contiditon="is not null"
    appid_contiditon="in($appids)"
    country_code_condition="in('us','cn')"
    #campaign_condition="in($campaigns)"
    campaign_condition="is not null"

    req_sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
    extra2, split(split(ext_algo, ',')[0],':')[0] as algo_cam,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, app_id, if(extra2!='', 'fill', 'no_fill') as if_fill,
    if(platform='ios',idfa,gaid) as user_id,
    ext_sysId, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(ext_algo, ',')[30]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[31]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and strategy like 'MNormalAlpha%'
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and app_id $appid_contiditon
    and split(split(ext_algo, ',')[0],':')[0] $campaign_condition
    and ad_type in('rewarded_video','interstitial_video')
    group by concat(yyyy,mm,dd), ad_type, unit_id, app_id, if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    extra2, split(split(ext_algo, ',')[0],':')[0],if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],
    if(platform='ios',idfa,gaid),ext_sysId"

    imp_sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    campaign_id, ext_campaignpackagename as package_name, split(split(ext_algo, ',')[0],':')[0] as algo_cam,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id,app_id,
    if(platform='ios',idfa,gaid) as user_id,ext_sysId,
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression_merge 
    where 
    concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date
    and split(ext_algo, ',')[1]>=$beg_date and split(ext_algo, ',')[1]<=$end_date 
    and strategy like 'MNormalAlpha%'
    and unit_id $unit_id_condition
    and app_id $appid_contiditon
    and lower(country_code) $country_code_condition
    and ad_type in('rewarded_video','interstitial_video')
    and split(split(ext_algo, ',')[0],':')[0] $campaign_condition
    group by concat(yyyy,mm,dd), ad_type, split(ext_algo, ',')[40],
    unit_id, app_id, if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    campaign_id, ext_campaignpackagename, split(split(ext_algo, ',')[0],':')[0],
    if(platform='ios',idfa,gaid),ext_sysId"

    sql="select a.*, b.campaign_id, b.imp, b.package_name 
    from($req_sql) a left join($imp_sql) b 
    on a.dtm=b.dtm 
    and a.ad_type=b.ad_type and a.country_code=b.country_code and a.unit_id=b.unit_id
    and a.user_id=b.user_id and a.ext_sysId=b.ext_sysId
    and a.app_id=b.app_id and a.algo_cam=b.algo_cam
    and a.if_lower_imp=b.if_lower_imp;"

    echo "sql:$sql"

    hive -e "$sql" >$output
}

query_waterfall_user_imp_req 2020100800 2020100823 &
