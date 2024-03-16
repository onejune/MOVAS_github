function query_moreoffer_effect_from_tracking() {
    beg_date=${1}
    end_date=${2}

    output=output/${beg_date}_${end_date}_rviv_moreoffer_effect.txt
    #unit_id_condition='in(447713,498064,452254,489072,435259,517857,450950,493509)'
    unit_id_condition='is not null'
	#app_id_condition='in(115392,146395,143686,145734,31222,138632)'
	app_id_condition='in(142256, 153512)'
	#app_id_condition='is not null'
    country_code_condition="in('us','cn')"
	country_code_condition='is not null'

    imp_sql="select 
        requestid,
        ext_adxrequestid, 
        concat(yyyy,mm,dd) as dtm, 
        unit_id,
		app_id,
        country_code, 
        campaign_id, 
        ad_type, 
        platform, 
        split(split(strategy, '\\;')[7], '-')[5] as strategy_name, 
        split(ext_algo, ',')[37] as big_template,
        split(ext_algo, ',')[14] as tpl_group,
        split(ext_algo, ',')[15] as video_tpl,
        split(ext_algo, ',')[16] as endcard_tpl,
        split(ext_algo, ',')[3] as cr_ivr,
        split(ext_algo, ',')[4] as cr_cpm,
        1 as imp,
        get_json_object(ext_dsp,'$.dspid') as dsp_id, 
        get_json_object(ext_dsp,'$.is_hb') as is_hb
        from dwh.ods_adn_trackingnew_impression_merge 
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and strategy like 'MNormalAlpha%'
        and adtype in('rewarded_video','interstitial_video')
        and unit_id $unit_id_condition
		and app_id $app_id_condition
        and lower(country_code) $country_code_condition"

    rviv_ins_sql="
        select 
        requestid, 
        split(ext_algo, ',')[2] as rev,
        1 as ins
        from dwh.ods_adn_trackingnew_install_merge
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and campaign_id!='223275881'
        and adtype in('rewarded_video','interstitial_video')
        "

    mf_ins_sql="select unit_id, ad_type, get_json_object(ext_data2,'$.crt_rid') as rviv_rid, 
        if(get_json_object(ext_bp,'$.[0]')!=0, get_json_object(ext_bp,'$.[0]'), split(ext_algo, ',')[2]) as rev, 
        1 as ins,
        campaign_id
        from dwh.ods_adn_trackingnew_install 
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and campaign_id!='223275881'
        and ad_type like 'more_offer'"

    sql="
        select 
            a.is_hb as is_hb, 
            a.country_code as country_code, 
            a.tpl_group as tpl_group,
            a.video_tpl as video_tpl,
            a.endcard_tpl as endcard_tpl,
			a.app_id as app_id,
            a.platform as platform, 
            a.ad_type as ad_type, 
            a.dsp_id as dsp_id, 
            sum(a.imp) as imp, 
            sum(b.rev) as rviv_rev, 
            sum(c.rev) as mf_rev
        from($imp_sql)a 
        left join ($rviv_ins_sql)b on a.requestid=b.requestid
        left join ($mf_ins_sql)c on a.ext_adxrequestid=c.rviv_rid
        group by 
            a.is_hb, 
            a.country_code, 
            a.tpl_group,
            a.video_tpl,
            a.endcard_tpl,
			a.app_id,
            a.platform, 
            a.ad_type, 
            a.dsp_id 
            ;"

    echo "$sql"
    hive -e "$sql" >$output
}

function query_rviv_moreoffer_imp_rate() {
    beg_date=${1}
    end_date=${2}

    output=output/${beg_date}_${end_date}_rviv_moreoffer_imp_rate.txt
    #unit_id_condition='in(447713,498064,452254)'
    unit_id_condition='is not null'
	app_id_condition='in(115392,146395,143686,145734,31222,138632)'
	app_id_condition='is not null'
    country_code_condition="in('us','cn')"
	#country_code_condition='is not null'

    rviv_imp_sql="select 
        ext_adxrequestid,
        requestid,
		campaign_id,
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
        split(split(ext_algo, ',')[26], '\004')[7] as mir, 
		split(ext_algo, ',')[4] as cr_cpm
        from dwh.ods_adn_trackingnew_impression_merge
        where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
        and adtype in('rewarded_video','interstitial_video')
        and unit_id $unit_id_condition
        and app_id $app_id_condition
		and lower(country_code) $country_code_condition
        "

    mf_imp_sql="select 
        get_json_object(ext_data2,'$.crt_rid') as rviv_rid,
        unit_id,
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
            a.app_id as app_id,
            a.country_code as country_code, 
            a.tpl_group as tpl_group,
            a.video_tpl as video_tpl,
            a.endcard_tpl as endcard_tpl,
            a.platform as platform, 
            a.ad_type as ad_type, 
            a.dsp_id as dsp_id,
            a.is_hb as is_hb,
            count(distinct a.requestid) as rviv_imp, 
            sum(b.imp) as mf_cam_imp,
			sum(a.mir) as mir,
            count(distinct b.rviv_rid) as mf_imp
        from($rviv_imp_sql)a 
        left join ($mf_imp_sql)b 
        on a.ext_adxrequestid=b.rviv_rid
        group by
            a.app_id,
            a.country_code, 
            a.tpl_group,
            a.video_tpl,
            a.endcard_tpl,
            a.platform, 
            a.ad_type, 
            a.dsp_id,
            a.is_hb
        ;"

    echo "$sql"
    hive -e "$sql" >$output
}


function query_effect_from_tracking_with_campaign() {
    beg_date=${1}
    end_date=${2}

    app_ids='142256,153512'
    publisher_ids='10714'
    output="output/${beg_date}_${end_date}_moreoffer_effect_from_tracking.txt"
    unit_id_condition='is not null'
    app_id_condition="in($app_ids)"
    country_code_condition="is not null"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, app_id,
	unit_id, campaign_id,
	split(split(strategy, '\\;')[7], '-')[5] as strategy_name,
	get_json_object(ext_dsp,'$.is_hb') as is_hb
    from dwh.ods_adn_trackingnew_impression
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('more_offer') 
    and app_id $app_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    get_json_object(ext_bp,'$.[0]') as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('more_offer') 
    "

    sql="select dtm, placement_id, publisher_id, app_id, country_code, ad_type, strategy_name, platform, is_hb,
	unit_id, campaign_id,
	count(requestid) as imp, 
    sum(rev) as rev, sum(ins) as ins
    from (select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id, a.app_id as app_id,
	a.dtm as dtm, a.ad_type as ad_type, 
	a.unit_id as unit_id, a.campaign_id as campaign_id,
	a.country_code as country_code, 
    a.strategy_name as strategy_name, 
    a.platform as platform,
    b.rev as rev, b.ins as ins, a.is_hb as is_hb
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=24*3600)c
    group by dtm, placement_id, ad_type, publisher_id, app_id, country_code, strategy_name, platform, is_hb,
	unit_id, campaign_id
	;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

function query_moreoffer_rev_from_tracking() {
    beg_date=${1}
    end_date=${2}
    output=output/${beg_date}_${end_date}_moreoffer_rev.txt

    sql="select concat(yyyy,mm,dd,hh) as dtm,
        ad_type, app_id, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc, 
        split(split(strategy, '\\;')[7], '-')[5] as strategy,
        count(*) as ins, 
		sum(get_json_object(ext_bp,'$.[0]')) as rev
        from dwh.ods_adn_trackingnew_install
        where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
        and strategy like 'MNormalAlpha%' and ad_type in('more_offer') 
        group by concat(yyyy,mm,dd,hh), 
        ad_type, app_id,
        if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
        split(split(strategy, '\\;')[7], '-')[5];
    "
    hive -e "$sql" > $output
}

#query_moreoffer_rev_from_tracking 2021122200 2021122223 &


#query_effect_from_tracking_with_campaign 2021121000 2021123123 &
#query_moreoffer_effect_from_tracking 2021121000 2021123123 &
query_rviv_moreoffer_imp_rate 2021122000 2021122423 &
