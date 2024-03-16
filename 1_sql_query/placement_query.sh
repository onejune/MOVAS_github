function query_placement_impression_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition="is not null"
    placement_id_condition="in('233894','233896')"
    country_code_condition="in('us','cn')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, ext_placementId, app_id, campaign_id,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    get_json_object(ext_dsp,'$.is_hb') as is_hb,
    unit_id, platform,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as imp
    from dwh.ods_adn_trackingnew_impression
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date)
    and unit_id $unit_id_condition
    and ext_placementId $placement_id_condition
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video','sdk_banner')
    group by concat(yyyy,mm,dd), ad_type, unit_id, ext_placementId, app_id, campaign_id,
    platform,if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    get_json_object(ext_dsp,'$.is_hb'),
    extra2, 
    if(idfa='00000000-0000-0000-0000-000000000000',0,1)
    ;"

    echo $sql
    hive -e "$sql" >output/campaign_impression_${beg_date}_${end_date}.txt
}

query_placement_impression_by_time 2020100100 2020101023
