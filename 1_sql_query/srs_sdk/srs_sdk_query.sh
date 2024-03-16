#########################################################################
# File Name: srs_sdk_query.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 29 Sep 2022 07:42:18 AM CST
#########################################################################
#!/bin/bash
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

function query_hb_request_by_time() {
    beg_date=${1}
    end_date=${2}

	unit_ids="'461649','1597036'"
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us')"
	#app_id_condition="in('149240','153467','134358', '154620')"
	app_id_condition="is not null"
	country_code_condition="in('us')"
	#country_code_condition="is not null"
	platform_condition="is in('android', 'ios')"
	dtm="concat(yyyy,mm,dd)"

    sql="select 
        $dtm as dtm,
    	if(lower(country_code) in('br', 'cn','us', 'jp', 'uk', 'id'),lower(country_code),'other') as country_code, 
        platform,
    	ad_type,
        CASE 
            WHEN ext_expids LIKE '%1861%' THEN '1861'
            WHEN ext_expids LIKE '%1862%' THEN '1862'
            WHEN ext_expids LIKE '%1864%' THEN '1864'
            ELSE 'unknown'
        END AS exp_id, 
        CASE 
            WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
            WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
            ELSE 'unknown'
        END AS client_type,
    	--split(split(ext_algo, ',')[45], '\004')[0] as region,
        sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
    	sum(split(split(ext_algo, ',')[25], '\004')[3]) as sum_mf_score,
    	sum(split(split(ext_algo, ',')[25], '\004')[4]) as sum_ori_score,
    	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    	sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cnrr_ivr,
        sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
        sum(split(ext_algo, ',')[2]) as sum_price,
        count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request
    where 
        (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    	and ad_type in('interstitial_video', 'rewarded_video')
        and lower(country_code) $country_code_condition
    	and app_id $app_id_condition
    	and unit_id $unit_id_condition
    group by 
        $dtm,
        platform,
    	ad_type,
        CASE 
            WHEN ext_expids LIKE '%1861%' THEN '1861'
            WHEN ext_expids LIKE '%1862%' THEN '1862'
            WHEN ext_expids LIKE '%1864%' THEN '1864'
            ELSE 'unknown'
        END, 
        CASE 
            WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
            WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
            ELSE 'unknown'
        END,
    	if(lower(country_code) in('br', 'cn','us', 'jp', 'uk', 'id'),lower(country_code),'other')
	"

    echo "$sql"
    tag="hb_request"
    output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}


#split(split(strategy, '\\;')[7], '-')[3] as prerank_strategy,
#split(split(ext_algo, ',')[30], '\004')[1] as cr_rank,

function query_imp_by_time() {
    beg_date=${1}
    end_date=${2}

	#unit_ids='1765321,1767811,1767304'
    #unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    #country_code_condition="in('us')"
	app_id_condition="in('132737', '127773', '126154', '127774')"
	app_id_condition="is not null"
    country_code_condition="is not null"
    country_code_condition="in('us')"
	dtm="concat(yyyy,mm,dd)"

    sql="select 
    $dtm as dtm,
	--substr(split(ext_algo, ',')[1], 1, 8) as req_time,
	unit_id,
    campaign_id,
	--split(split(ext_algo, ',')[45], '\004')[0] as region,
	if(lower(country_code) in('cn','us', 'jp', 'uk', 'id'),lower(country_code),'other') as country_code,
	split(ext_algo, ',')[6] as cpx,
    --split(split(ext_algo, ',')[30], '\004')[0] as recall_rank,
    CASE 
        WHEN ext_expids LIKE '%1861%' THEN '1861'
        WHEN ext_expids LIKE '%1862%' THEN '1862'
        WHEN ext_expids LIKE '%1864%' THEN '1864'
        ELSE 'unknown'
    END AS exp_id, 
    CASE 
        WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
        WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
        ELSE 'unknown'
    END AS client_type,
    split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
	split(ext_algo, ',')[56] as is_delay_calibra,
	split(ext_algo, ',')[75] as delay_rate,
	split(ext_algo, ',')[60] as ppr_ratio,
    --split(split(ext_algo, ',')[58], '\004')[1] as mof_ratio,
    get_json_object(ext_dsp,'$.is_hb') as is_hb,
    split(ext_algo, ',')[8] as trynew_type,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    count(*) as imp
    from dwh.ods_adn_trackingnew_ssp_impression
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
	and ad_type in('rewarded_video','interstitial_video')
    and lower(country_code) $country_code_condition
	and app_id $app_id_condition
	and unit_id $unit_id_condition
	and campaign_id in('377088491','377797911','377725975')
	and strategy like '%MNormalAlphaModelRanker%'
    group by 
    $dtm,
	--substr(split(ext_algo, ',')[1], 1, 8),
    campaign_id,
	unit_id,
	split(ext_algo, ',')[60],
	split(ext_algo, ',')[6],
    --split(split(ext_algo, ',')[30], '\004')[0],
	--split(split(ext_algo, ',')[45], '\004')[0],
    CASE 
        WHEN ext_expids LIKE '%1861%' THEN '1861'
        WHEN ext_expids LIKE '%1862%' THEN '1862'
        WHEN ext_expids LIKE '%1864%' THEN '1864'
        ELSE 'unknown'
    END,
    CASE 
        WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
        WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
        ELSE 'unknown'
    END, 
    get_json_object(ext_dsp,'$.is_hb'),
    split(ext_algo, ',')[8],
	if(lower(country_code) in('cn','us', 'jp', 'uk', 'id'),lower(country_code),'other'),
    split(split(strategy, '\\;')[7], '-')[1],
    --split(split(ext_algo, ',')[58], '\004')[1],
    split(ext_algo, ',')[56],
    split(ext_algo, ',')[75],
	split(ext_algo, ',')[14],
	split(ext_algo, ',')[15]
	"

    echo "$sql"
    tag="unit_imp_by_time"
    output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
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
    #app_id_condition='in($app_ids)'
    app_id_condition='is not null'
    country_code_condition="in('us', 'cn')"
    #country_code_condition='is not null'
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, 
            if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
            publisher_id, 
        	app_id, unit_id, campaign_id,
            CASE 
                WHEN ext_expids LIKE '%1861%' THEN '1861'
                WHEN ext_expids LIKE '%1862%' THEN '1862'
                WHEN ext_expids LIKE '%1864%' THEN '1864'
                ELSE 'unknown'
            END AS exp_id, 
            CASE 
                WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
                WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
                ELSE 'unknown'
            END AS client_type,
        	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
        	split(ext_algo, ',')[37] as big_template,
        	split(ext_algo, ',')[14] as tpl_group,
        	split(ext_algo, ',')[15] as video_tpl,
        	split(ext_algo, ',')[16] as endcard_tpl,
            split(ext_algo, ',')[3] as cr_ivr,
            split(ext_algo, ',')[4] as cr_cpm,
            split(split(ext_algo, ',')[29], '\004')[0] as bid_price,
        	split(ext_algo, ',')[40] as if_lower_imp,
        	split(ext_algo, ',')[35] as btl_hit_tag,
        	if(split(split(ext_algo, ',')[26], '\004')[7]==0, 0, 1) as cerr_valid,
        	split(split(ext_algo, ',')[19], '\004')[5] as pr_moct_cnt,
        	split(split(ext_algo, ',')[26], '\004')[5] as ttgi_ivr,
        	split(split(ext_algo, ',')[26], '\004')[4] as cnrr_ivr,
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
        	split(ext_algo, ',')[2] as price,
            1 as imp
        from dwh.ods_adn_trackingnew_ssp_impression
    	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date}
            and ad_type in('rewarded_video','interstitial_video') 
            and unit_id $unit_id_condition
            and app_id $app_id_condition
        	and publisher_id $publisher_id_condition
            and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
        split(ext_algo, ',')[2] as rev,
    	1 as ins
        from dwh.ods_adn_trackingnew_install
        where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
        and ad_type in('rewarded_video','interstitial_video')
        "

	sql="
        select 
            a.dtm,
        	a.strategy_name,
            a.ad_type,
        	--a.app_id,
            --a.unit_id,
        	--a.big_template,
        	a.country_code,
            a.platform, 
            a.is_hb,
            a.client_type,
            a.exp_id,
            sum(a.cr_cpm) as cr_cpm, 
            sum(a.cr_ivr) as cr_ivr, 
            sum(a.fr_cpm) as fr_cpm,
        	sum(a.cnrr_ivr) as cnrr_ivr,
            sum(a.bid_price) as bid_price,
            sum(a.price) as price,
			sum(a.imp) as imp,
            sum(b.ins) as ins,
            sum(b.rev) as rev
        from(
            ($imp_sql) a left join ($ins_sql) b on (a.requestid=b.requestid and b.created-a.created<=12*3600)
        )
    	group by 
            a.dtm,
            a.strategy_name,
            a.ad_type,
            --a.app_id,
            --a.unit_id,
            --a.big_template,
            a.country_code,
            a.platform, 
            a.is_hb,
            a.client_type,
            a.exp_id
    	;"

    echo "$sql"
    tag="copc_cpm"
    output="output/${tag}_${beg_date}_${end_date}.txt"
    spark_submit_job "$sql" $tag $output
}


#query_unit_fillrate_hourly 2022013110 2022013115 &

query_hb_request_by_time 2022093000 2022100123
#query_imp_by_time 2022092900 2022100123 &
#query_copc 2022092900 2022100123 &



