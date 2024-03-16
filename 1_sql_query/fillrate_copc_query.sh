export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf

function spark_submit_job() {
    sql=$1
    tag=$2
    output=$3
    hive_out="s3://mob-emr-test/wanjun/hive_out/temp_temp/$tag"
    if [ -z "$sql" ] || [ -z "$tag" ] || [ -z "$output" ]; then
        echo "ERROR: input is valid, exit !"
        exit
    fi
	hadoop fs -rmr $hive_out
    echo "sql: $sql"

    spark-submit \
    --deploy-mode cluster \
    --num-executors 300 \
    --executor-cores 4 \
	--driver-cores 4 \
    --driver-memory 10G \
    --executor-memory 10G \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=350 \
    --conf spark.core.connection.ack.wait.timeout=300 \
    --class org.mobvista.dataplatform.SubmitSparkSql s3://mob-emr/adn/k8s_spark_migrate/release/spark-sql-submitter-1.0.0.jar \
    "${sql}"  \
    "${hive_out}"  \
    "1" \
    Overwrite \
    csv \
	"\t" \
	true

    hadoop fs -text $hive_out/* > $output
}

#split(split(ext_algo, ',')[0], ':')[0] as algo_cam,

function query_fillrate_by_time() {
    beg_date=${1}
    end_date=${2}

	unit_ids="'461649','1597036'"
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us')"
	#app_id_condition="in('149240','153467','134358', '154620')"
	app_id_condition="is not null"
	country_code_condition="in('us')"
	country_code_condition="is not null"
	platform_condition="is in('android', 'ios')"
	dtm="concat(yyyy,mm,dd)"

    sql="select 
    $dtm as dtm,
	if(lower(country_code) in('br', 'cn','us', 'jp', 'uk', 'id'),lower(country_code),'other') as country_code, 
    platform,
	split(split(ext_algo, ',')[45], '\004')[0] as region,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(split(ext_algo, ',')[26], '\004')[5]) as sum_ttgi_ivr,
	sum(split(split(ext_algo, ',')[25], '\004')[3]) as sum_mf_score,
	sum(split(split(ext_algo, ',')[25], '\004')[4]) as sum_ori_score,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_hb_v1_bid
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
	and ad_type in('interstitial_video')
    and lower(country_code) $country_code_condition
	and app_id $app_id_condition
	and unit_id $unit_id_condition
	and extra3 like '%MNormalAlphaModelRankerHH%'
    group by 
    $dtm,
    platform,
	split(split(ext_algo, ',')[45], '\004')[0],
	if(lower(country_code) in('br', 'cn','us', 'jp', 'uk', 'id'),lower(country_code),'other')
	"

    echo "$sql"
    tag="unit_fillrate_by_time1"
    output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}


#split(split(strategy, '\\;')[7], '-')[3] as prerank_strategy,
#split(split(ext_algo, ',')[30], '\004')[1] as cr_rank,

function query_imp_by_time() {
    beg_date=${1}
    end_date=${2}

	unit_ids='1765321,1767811,1767304'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us')"
	app_id_condition="in('132737', '127773', '126154', '127774')"
	app_id_condition="is not null"
    country_code_condition="is not null"
    #country_code_condition="in('us')"
	platform_condition="is in('android', 'ios')"
	dtm="concat(yyyy,mm,dd)"

    sql="select 
    $dtm as dtm,
	split(split(ext_algo, ',')[45], '\004')[0] as region,
	if(lower(country_code) in('cn','us', 'jp', 'uk', 'id'),lower(country_code),'other') as country_code, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    count(*) as imp
    from dwh.ods_adn_trackingnew_impression
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
	and ad_type in('rewarded_video','interstitial_video')
    and lower(country_code) $country_code_condition
	and app_id $app_id_condition
	and unit_id $unit_id_condition
	and strategy like '%MNormalAlphaModelRanker%'
    group by 
    $dtm,
	split(split(ext_algo, ',')[45], '\004')[0],
	if(lower(country_code) in('cn','us', 'jp', 'uk', 'id'),lower(country_code),'other')
	"

    echo "$sql"
    tag="unit_imp_by_time"
    output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}




function query_unit_fillrate_hourly() {
    begin_date=$1
    end_date=$2

    unit_ids='1579784'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    country_code_condition="in('us', 'cn', 'jp', 'uk')"
	#country_code_condition='is not null'

    sql="select 
	concat(yyyy,mm,dd,hh) as dtm,
	substr(split(ext_algo, ',')[1], 1, 8) as day,
	ad_type,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
    if(extra2!='', 'fill', 'no_fill') as if_fill, split(ext_algo, ',')[40] as if_lower_imp, 
    split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
	split(ext_algo, ',')[23] as flow_type,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
	sum(split(split(ext_algo, ',')[26], '\004')[7]) as sum_mir_cpm,
	sum(split(split(ext_algo, ',')[26], '\004')[8]) as sum_emor_cpm,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
	sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date
    and unit_id $unit_id_condition
	and strategy like '%MNormalAlpha%'
    and lower(country_code) $country_code_condition
	and ad_type in('rewarded_video','interstitial_video')
    group by
	concat(yyyy,mm,dd,hh),
	substr(split(ext_algo, ',')[1], 1, 8),
	ad_type, if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	split(ext_algo, ',')[23],
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
    split(split(strategy, '\\;')[7], '-')[1];"

    hive -e "$sql" >output/unit_hourly_fillrate_${begin_date}_${end_date}.txt
}

function query_profit_control_fillrate_hourly() {
    begin_date=$1
    end_date=$2

    aws s3 cp s3://mob-emr-test/xiaoyang/adserver/fillrate_control/conf/m_sys_profit_info.txt data/expect_roi
    expect_roi_file="data/expect_roi"
    expect_roi_list=""
    OLD_IFS=$IFS
    IFS=$'\n'
    for line in $(cat ${expect_roi_file}); do
        unit_cc=$(echo ${line} | awk -F" " '{print $1}')
        if [[ -n "${expect_roi_list}" ]]; then
            expect_roi_list=${expect_roi_list}",'"${unit_cc}"'"
        else
            expect_roi_list="'"${unit_cc}"'"
        fi
    done
    echo ${expect_roi_list}
    IFS=$OLD_IFS

    unit_id_condition="concat_ws('_', unit_id, lower(country_code)) in (${expect_roi_list})"
    country_code_condition="in('us','cn')"

    sql="select substr(split(ext_algo, ',')[1], 1, 8) as dtm,
    split(ext_algo, ',')[1] as hour, split(ext_algo, ',')[18] as rs_flag, ad_type,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, split(ext_algo, ',')[40] as if_lower_imp, 
    split(split(strategy, '\\;')[7], '-')[0] as strategy_name, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price, 
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge 
    where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date
    and split(ext_algo, ',')[1]>=$begin_date and split(ext_algo, ',')[1]<=$end_date
    and strategy like 'MNormalAlpha%'
    and $unit_id_condition
    and lower(country_code) $country_code_condition
    group by substr(split(ext_algo, ',')[1], 1, 8), split(ext_algo, ',')[1], 
    split(ext_algo, ',')[18], ad_type, unit_id, if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
    split(split(strategy, '\\;')[7], '-')[0];"

    hive -e "$sql" >output/profit_control_hourly_fillrate_${begin_date}_${end_date}.txt
}

function query_campaign_fillrate_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_ids='488099'
    unit_id_condition="in($unit_ids)"
	unit_id_condition='is not null'
    country_code_condition="in('us','cn')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
	campaign_id,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
	split(ext_algo, ',')[16] as end_tpl,
	split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
	split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
    extra2, split(ext_algo, ',')[0] as algo_cam,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    app_id, unit_id, platform, if(extra2!='', 'fill', 'no_fill') as if_fill, 
    split(split(strategy, '\\;')[7], '-')[0] as strategy_name, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request_merge 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and ext_algo like '%374156865%'
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video','sdk_banner')
    group by concat(yyyy,mm,dd), ad_type, 
	app_id, unit_id, platform, campaign_id,
	split(ext_algo, ',')[14],
	split(ext_algo, ',')[15],
	split(ext_algo, ',')[16],
	split(split(ext_algo, ',')[20], '\004')[0],
	split(split(ext_algo, ',')[20], '\004')[1],
	split(split(ext_algo, ',')[20], '\004')[2],
	if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    extra2, split(ext_algo, ',')[0],if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    split(split(strategy, '\\;')[7], '-')[0];"

    echo $sql
    hive -e "$sql" >output/campaign_fillrate_${beg_date}_${end_date}.txt
}


function query_copc() {
    beg_date=${1}
    end_date=${2}

    unit_ids=''
	app_ids='151596,152159,151237,127773'
    publisher_ids='10714'
    output=output/${beg_date}_${end_date}_copc_from_tracking.txt
    unit_id_condition='is not null'
    #unit_id_condition='in($unit_ids)'
    app_id_condition='in($app_ids)'
    country_code_condition="in('us', 'cn')"
    #country_code_condition='is not null'
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, 
	app_id, unit_id, campaign_id,
	split(split(strategy, '\\;')[7], '-')[3] as strategy_name,
	split(ext_algo, ',')[37] as big_template,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
	split(ext_algo, ',')[16] as endcard_tpl,
    split(ext_algo, ',')[3] as cr_ivr,
    split(ext_algo, ',')[4] as cr_cpm,
	split(ext_algo, ',')[40] as if_lower_imp,
	split(ext_algo, ',')[35] as btl_hit_tag,
	if(split(split(ext_algo, ',')[26], '\004')[7]==0, 0, 1) as cerr_valid,
	split(split(ext_algo, ',')[19], '\004')[5] as pr_moct_cnt,
	split(split(ext_algo, ',')[26], '\004')[5] as ttgi_ivr,
	split(split(ext_algo, ',')[26], '\004')[4] as cnrr_ivr,
	split(split(ext_algo, ',')[24], '\004')[0] as imp_ration,
	split(split(ext_algo, ',')[25], '\004')[1] as fr_cpm,
	split(split(ext_algo, ',')[19], '\004')[2] as video_cnt,
	split(split(ext_algo, ',')[19], '\004')[3] as playable_cnt,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	split(ext_algo, ',')[8] as trynew_type,
	split(ext_algo, ',')[36] as btl_cnt,
	split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(split(ext_algo, ',')[20], '\004')[1] as video_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] as playable_trynew,
	split(split(ext_algo, ',')[20], '\004')[3] as img_trynew,
	split(ext_algo, ',')[2] as price
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date}
	and(substr(split(ext_algo, ',')[1],1,10) between ${beg_date} and ${end_date})
    and adtype in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
    and app_id $app_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and(substr(split(ext_algo, ',')[1],1,10) between ${beg_date} and ${end_date})
    and adtype in('rewarded_video','interstitial_video')
    "
	sql="select dtm, publisher_id, if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, ad_type, app_id,
	is_hb,if_lower_imp,btl_cnt,strategy_name,
	big_template,
	btl_hit_tag,
	platform,
	campaign_id,
	count(requestid) as imp, sum(cr_cpm) as cr_cpm, sum(fr_cpm) as fr_cpm, sum(cr_ivr) as cr_ivr, sum(rev) as rev, sum(ins) as ins,
	sum(ttgi_ivr) as ttgi_ivr,
	sum(cnrr_ivr) as cnrr_ivr
    from 
	(select a.requestid as requestid, a.publisher_id as publisher_id, 
	a.strategy_name as strategy_name,
	a.app_id as app_id,a.unit_id as unit_id,
	a.if_lower_imp as if_lower_imp,
	a.btl_hit_tag as btl_hit_tag,
	a.dtm as dtm, a.ad_type as ad_type, a.imp_ration as imp_ration, a.price as price, 
	a.big_template as big_template,
	a.btl_cnt as btl_cnt,
	a.campaign_id as campaign_id,
	a.tpl_group as tpl_group,
	a.video_tpl as video_tpl,
	a.endcard_tpl as endcard_tpl,
	a.country_code as country_code,
    a.platform as platform, a.cr_cpm as cr_cpm, a.cr_ivr as cr_ivr, a.fr_cpm as fr_cpm,
	a.ttgi_ivr as ttgi_ivr,
	a.cnrr_ivr as cnrr_ivr,
    b.rev as rev, b.ins as ins, a.is_hb as is_hb
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=12*3600)c
	group by dtm, ad_type, publisher_id, if(lower(country_code) in('cn','us'),lower(country_code),'other'),
	app_id,btl_cnt,strategy_name,
	platform,
	campaign_id,
	btl_hit_tag,
	big_template,
	is_hb,if_lower_imp
	;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}


#query_copc_with_campaign_creative 2020102800 2020103006 &
#query_copc_with_campaign 2020123000 2021123123 &
#query_campaign_fillrate_by_time 2021080200 2021080923 &
#query_fillrate_day_and_day 2021032500 2021032510 &
#query_copc_day_and_day 2020062300 2020062306 &
#query_predict_and_cpm_by_time 2020091700 2020091723 &
#query_unit_fillrate_hourly 2022013110 2022013115 &

query_fillrate_by_time 2022071800 2022072523 

#query_imp_by_time 2022071800 2022072123

#query_campaign_fillrate_by_time 2021122000 2021122623 &
#query_copc_with_campaign_creative 2020111100 2020111423 &
#query_copc 2021110900 2021110909 &
