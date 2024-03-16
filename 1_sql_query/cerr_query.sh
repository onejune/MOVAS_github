function query_creative_cnt() {
    beg_date=$1
    end_date=$2

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition="is not null"
	unit_ids='104819'
	unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"

    sql="select
    substr(split(ext_algo, ',')[1],1,8) as dtm, 
	ad_type,
	campaign_id, ext_campaignpackagename,
	if(extra2!='', 'fill', 'no_fill') as if_fill,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
	split(split(strategy, '\\;')[7], '-')[1] as strategy,
	split(split(strategy, '\\;')[2], '_')[4] as cerr_scource,
	split(split(ext_algo, ',')[34], '\004')[1] as video_id,
	split(split(ext_algo, ',')[34], '\004')[2] as playable_id,
	if(split(split(strategy, '\\;')[2], '_')[1]='0', 0, 1) as video_hit,
	if(split(split(strategy, '\\;')[2], '_')[2]='0', 0, 1) as playable_hit,
    sum(split(split(ext_algo, ',')[19], '\004')[2]) as video_cnt,
    sum(split(split(ext_algo, ',')[19], '\004')[3]) as playable_cnt,
    sum(split(split(ext_algo, ',')[19], '\004')[5]) as pr_moct_cnt,
    sum(split(split(ext_algo, ',')[19], '\004')[6]) as valid_cerr_cnt,
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression
    where
    concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by 
    substr(split(ext_algo, ',')[1],1,8), 
	ad_type,
	campaign_id, ext_campaignpackagename,
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	split(split(strategy, '\\;')[7], '-')[1],
	if(extra2!='', 'fill', 'no_fill'),
	split(split(strategy, '\\;')[2], '_')[4],
	split(split(ext_algo, ',')[34], '\004')[1],
	split(split(ext_algo, ',')[34], '\004')[2],
	if(split(split(strategy, '\\;')[2], '_')[1]='0', 0, 1),
	if(split(split(strategy, '\\;')[2], '_')[2]='0', 0, 1)
	;"

    echo "$sql"
	hive -e "$sql" >output/creative_cnt_${beg_date}_${end_date}.txt
}

function query_copc_cerr() {
    beg_date=${1}
    end_date=${2}

    unit_ids='198608,391297,415349'
    publisher_ids=''
    output="output/${beg_date}_${end_date}_cerr_copc_from_tracking.txt"
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, 
	app_id, unit_id,
	campaign_id,ext_campaignpackagename,
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
    split(ext_algo, ',')[3] as cr_ivr,
    split(ext_algo, ',')[4] as cr_cpm,
    if(split(split(strategy, '\\;')[2], '_')[1]='0', 0, 1) as video_hit,
	if(split(split(strategy, '\\;')[2], '_')[2]='0', 0, 1) as playable_hit,
	split(split(strategy, '\\;')[2], '_')[4] as cerr_scource,
	if(split(split(ext_algo, ',')[26], '\004')[7]==0, 0, 1) as cerr_valid,
	split(split(ext_algo, ',')[19], '\004')[5] as pr_moct_cnt,
	split(split(ext_algo, ',')[24], '\004')[0] as imp_ration,
	split(split(ext_algo, ',')[25], '\004')[1] as fr_cpm,
	split(split(ext_algo, ',')[19], '\004')[2] as video_cnt,
	split(split(ext_algo, ',')[19], '\004')[3] as playable_cnt,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	split(ext_algo, ',')[8] as trynew_type,
	split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(ext_algo, ',')[2] as price
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date}
	and(substr(split(ext_algo, ',')[1],1,10) between ${beg_date} and ${end_date})
    and ad_type in('rewarded_video','interstitial_video','more_offer') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and(substr(split(ext_algo, ',')[1],1,10) between ${beg_date} and ${end_date})
    and ad_type in('rewarded_video','interstitial_video','more_offer')
    "
    sql="select dtm, publisher_id, country_code, ad_type, app_id,
	strategy_name, is_hb,
    video_hit,
    playable_hit,
	package_name,
	cerr_scource,
	count(requestid) as imp, sum(cr_cpm) as cr_cpm, sum(fr_cpm) as fr_cpm, sum(cr_ivr) as cr_ivr, sum(rev) as rev, sum(ins) as ins,
	sum(pr_moct_cnt) as sum_moct_cnt,
	sum(video_cnt) as sum_video_cnt,
	sum(playable_cnt) as sum_playable_cnt,
	sum(imp_ration) as imp_ration
    from 
	(select a.requestid as requestid, a.publisher_id as publisher_id, 
	a.strategy_name as strategy_name,
	a.app_id as app_id,a.unit_id as unit_id,
	a.dtm as dtm, a.ad_type as ad_type, a.imp_ration as imp_ration, a.price as price, 
    a.trynew_type as trynew_type,
	a.tpl_trynew as tpl_trynew,
	a.cerr_valid as cerr_valid,
	a.is_hb as is_hb,
	a.country_code as country_code,
	a.ext_campaignpackagename as package_name,
	a.campaign_id as campaign_id,
	a.cerr_scource as cerr_scource,
    a.platform as platform, a.cr_cpm as cr_cpm, a.cr_ivr as cr_ivr, a.fr_cpm as fr_cpm,
	a.pr_moct_cnt as pr_moct_cnt,
	a.playable_cnt as playable_cnt,
	a.video_cnt as video_cnt,
    a.video_hit as video_hit,
    a.playable_hit as playable_hit,
    b.rev as rev, b.ins as ins
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=12*3600)c
    group by dtm, ad_type, publisher_id, country_code,
	package_name,
	is_hb,
    video_hit,
    playable_hit,
	cerr_scource,
	app_id, strategy_name
	having
	strategy_name in('0_cnrr2_exp', '0_cerr_exp', '0_nn_base');"

    #sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
    #from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

function query_recall_rate() {
    beg_date=$1
    end_date=$2

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition="is not null"
    country_code_condition="is not null"

    sql="select
    concat(yyyy,mm,dd) as dtm, ad_type,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
    unit_id,
    split(split(strategy, '\\;')[7], '-')[1] as strategy,
    split(split(strategy, '\\;')[2], '_')[4] as cerr_source,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(ext_algo, ',')[2]) as rev,
    count(*) as ins 
    from dwh.ods_adn_trackingnew_install
    where
    concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and split(strategy, '\\;')[3]!='1'
    group by concat(yyyy,mm,dd), ad_type,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
    unit_id,
	split(split(strategy, '\\;')[2], '_')[4],
	split(split(strategy, '\\;')[7], '-')[1]
    ;"

    hive -e "$sql" >output/recall_rate_${beg_date}_${end_date}.txt
}

#query_recall_rate 2021041300 2021041623
query_creative_cnt 2021042000 2021042023
#query_copc_cerr 2021032700 2021032923
