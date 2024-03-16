function query_rviv_moreoffer_label() {
    beg_date=${1}
    end_date=${2}

    output=output/${beg_date}_${end_date}_rviv_moreoffer_label.txt
    unit_id_condition='in(489072)'
    #unit_id_condition='is not null'
    country_code_condition="in('us')"

    rviv_imp_sql="select 
        ext_adxrequestid,
        requestid,
        concat(yyyy,mm,dd) as dtm, 
        app_id,
        unit_id, 
        country_code, 
        ad_type, 
        platform, 
        split(split(strategy, '\\;')[7], '-')[0] as strategy_name,
        split(ext_algo, ',')[37] as big_template,
        split(ext_algo, ',')[14] as tpl_group,
        split(ext_algo, ',')[15] as video_tpl,
        split(ext_algo, ',')[16] as endcard_tpl,
        get_json_object(ext_dsp,'$.dspid') as dsp_id, 
        get_json_object(ext_dsp,'$.is_hb') as is_hb,
        split(ext_algo, ',')[3] as cr_ivr,
        split(ext_algo, ',')[4] as cr_cpm
        from dwh.ods_adn_trackingnew_impression_merge
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and adtype in('rewarded_video','interstitial_video')
        and unit_id $unit_id_condition
        and lower(country_code) $country_code_condition
        "

    mf_imp_sql="select 
        get_json_object(ext_data2,'$.crt_rid') as rviv_rid,
        unit_id,
        1 as is_imp,
        count(requestid) as imp
        from dwh.ods_adn_trackingnew_impression 
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and ad_type in('more_offer')
        and lower(country_code) $country_code_condition
        group by
        get_json_object(ext_data2,'$.crt_rid'),
        unit_id
        "

    sql="
        select 
            a.dtm as dtm,
            a.app_id as app_id,
            a.country_code as country_code, 
            a.tpl_group as tpl_group,
            a.video_tpl as video_tpl,
            a.endcard_tpl as endcard_tpl,
            a.unit_id as unit_id, 
            a.platform as platform, 
            a.ad_type as ad_type, 
            b.unit_id as mf_unit,
            a.dsp_id as dsp_id,
            a.is_hb as is_hb,
            b.is_imp as is_imp,
            b.imp as imp
        from($rviv_imp_sql)a 
        left join ($mf_imp_sql)b 
        on a.ext_adxrequestid=b.rviv_rid
        ;"

    echo "$sql"
    hive -e "$sql" >$output
}

query_rviv_moreoffer_label 2021082000 2021082023
