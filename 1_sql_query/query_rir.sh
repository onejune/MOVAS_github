
function query_rir() {
    beg_date=${1}
    end_date=${2}
    req_type=${3}
    day_on_day=${4}

    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    if [[ $day_on_day -eq "1" ]]; then
        time_range="((concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
        or (concat(yyyy,mm,dd,hh)>=$beg_date1 and concat(yyyy,mm,dd,hh)<=$end_date1)
        or (concat(yyyy,mm,dd,hh)>=$beg_date2 and concat(yyyy,mm,dd,hh)<=$end_date2))"
        output="output/${beg_date}_${end_date}_rir_${req_type}_day_on_day.txt"
    else
        time_range="(concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date)"
        output="output/${beg_date}_${end_date}_rir_${req_type}_now.txt"
    fi

    if [[ $req_type -eq 'hb' ]]; then
        req_tb="dwh.ods_adn_trackingnew_hb_request"
        ad_type_field="ad_type"
    else
        req_tb="dwh.ods_adn_trackingnew_request_merge"
        ad_type_field="adtype"
    fi

    app_ids='132737'
    publisher_ids='10714'
	unit_ids='461649,511106'
    #unit_id_condition='is not null'
	unit_id_condition="in($unit_ids)"
    #app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    country_code_condition="in('us', 'cn', 'jp', 'uk')"
    #country_code_condition='is not null'
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    req_sql="
        select requestid,
        concat(yyyy,mm,dd) as dtm,
        ad_type,
        platform, 
        publisher_id,
        ext_placementId, 
        if(lower(country_code) in('cn','us'), lower(country_code),'other') as country_code,
        app_id,
        unit_id,
		split(split(ext_algo, ',')[0], ':')[0] as campaign_id,
        split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
        split(ext_algo, ',')[37] as big_template,
		split(split(ext_algo, ',')[54], ';')[0] as is_emor,
        split(ext_algo, ',')[14] as tpl_group,
        split(ext_algo, ',')[15] as video_tpl,
        split(ext_algo, ',')[3] as cr_ivr,
        split(ext_algo, ',')[4] as cr_cpm,
		split(split(ext_algo, ',')[26], '\004')[6] as mir,
		split(split(ext_algo, ',')[26], '\004')[7] as emor_cpm,
        split(ext_algo, ',')[40] as if_lower_imp,
        split(ext_algo, ',')[8] as trynew_type,
        split(ext_algo, ',')[36] as btl_cnt,
        split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
        split(split(ext_algo, ',')[20], '\004')[1] as video_trynew,
        split(split(ext_algo, ',')[20], '\004')[2] as playable_trynew,
        split(split(ext_algo, ',')[20], '\004')[3] as img_trynew,
        split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
        split(ext_algo, ',')[2] as price
        from $req_tb
        where $time_range
        and $ad_type_field in('rewarded_video','interstitial_video') 
        and unit_id $unit_id_condition
        and app_id $app_id_condition
        and publisher_id $publisher_id_condition
        and lower(country_code) $country_code_condition
		and strategy like '%MNormalAlpha%'
        "

    imp_sql="
        select requestid,
        1 as imp
        from dwh.ods_adn_trackingnew_impression_merge
        where $time_range
        and adtype in('rewarded_video','interstitial_video') 
        and unit_id $unit_id_condition
        and publisher_id $publisher_id_condition
        and lower(country_code) $country_code_condition
        "

    ins_sql="
        select 
        requestid, 
        split(ext_algo, ',')[2] as rev,
        1 as ins
        from dwh.ods_adn_trackingnew_install_merge
        where $time_range
        and adtype in('rewarded_video','interstitial_video')
        "

	sql="
        select 
        a.dtm as dtm,
		a.campaign_id as campaign_id,
        a.country_code as country_code, 
        a.ad_type as ad_type,
		a.platform as platform,
		a.is_emor as is_emor,
		a.app_id as app_id,
        count(a.requestid) as req, 
        sum(a.cr_cpm) as sum_cr_cpm, 
        sum(a.price) as sum_price,
		sum(a.mir) as sum_mir,
		sum(a.emor_cpm) as sum_emor_cpm,
        sum(a.max_fillrate_score) as sum_mfs,
        sum(b.imp) as imp, 
        sum(c.rev) as rev, 
        sum(c.ins) as ins
        from 
        ($req_sql) a 
        left join ($imp_sql) b on a.requestid=b.requestid
        left join ($ins_sql) c on b.requestid=c.requestid
        group by 
        a.dtm, a.ad_type, 
		a.campaign_id,
		a.country_code,
		a.is_emor,
		a.app_id,
		a.platform
        ;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

start_time=2022022700
end_time=2022022716
req_type=hb
is_day_on_day=1

query_rir $start_time $end_time $req_type $is_day_on_day
