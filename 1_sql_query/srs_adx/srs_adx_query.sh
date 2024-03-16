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
    --driver-memory 8G \
    --executor-memory 8G \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=300 \
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

function query_adx_imp() {
    beg_date=${1}
    end_date=${2}

    country_code_condition="in('US')"
    #country_code_condition="is not null"
    dtm="concat(yyyy,mm,dd)"

    sql="
        select 
        concat(yr,mt,dt) as day,
        exchanges,countrycode,os,
        get_json_object(ext10,'$.reqtype') as adtype,
        get_json_object(get_json_object(ext3,'$.abtest'),'$.only_saas') as abtag,
        case when get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%' then 'saasAdx' 
            when ext8 like '%,RTDSP%' then 'rtdsp' 
            else 'drs' end as sub_algo,
        split(split(ext4, ',')[45], '\004')[0] as region,
		split(get_json_object(ext3,'$.creative'),'#')[3] as tempid,
		count(*) as imp,
        sum(price) as cost,
        sum(cinstallprice) as cinstall_price,
        sum(ifnull(float(get_json_object(ext3,'$.ivr')),0.0)) as sum_ivr
        from adn_dsp.log_adn_dsp_impression_hour
        where 
        concat(yr,mt,dt,hh)>='$beg_date' and concat(yr,mt,dt,hh)<='$end_date'
        and ext2='rank_model_camp'
        and get_json_object(ext3,'$.abtest') is not null
		and countrycode $country_code_condition
        group by
        concat(yr,mt,dt),
        exchanges,countrycode,os,
        get_json_object(ext10,'$.reqtype'),
        get_json_object(get_json_object(ext3,'$.abtest'),'$.only_saas'),
        case when get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%' then 'saasAdx' 
            when ext8 like '%,RTDSP%' then 'rtdsp' 
            else 'drs' end,
        split(split(ext4, ',')[45], '\004')[0],
		split(get_json_object(ext3,'$.creative'),'#')[3]
    "

    echo "$sql"
    tag="adx-imp"
    output="output/${tag}_${beg_date}_${end_date}.txt"
    spark_submit_job "$sql" $tag $output
}

function query_adx_cpm() {
    beg_date=${1}
    end_date=${2}

    country_code_condition="in('US')"
    country_code_condition="is not null"

    imp_sql="
        select 
        requestid,
        concat(yr,mt,dt) as dtm,
        exchanges,countrycode,os,
        get_json_object(ext10,'$.reqtype') as adtype,
        get_json_object(get_json_object(ext3,'$.abtest'),'$.only_saas') as abtag,
        case when get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%' then 'saasAdx' 
            when ext8 like '%,RTDSP%' then 'rtdsp' 
            else 'drs' end as sub_algo,
		campaignid,
		cpackagename as packagename,
		split(get_json_object(ext3,'$.creative'),'#')[3] as tempid,
		split(split(get_json_object(ext3,'$.strategy'), '\\;')[7], '-')[0] as srs_strategy,
		price,
		bid as bid_price,
        cinstallprice,
        --float(get_json_object(ext3,'$.ivr')) as p_ivr,
		if(get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%', float(split(ext4, ',')[3]) * 1000, float(get_json_object(ext3,'$.ivr'))) as p_ivr,
		1 as imp
        from adn_dsp.log_adn_dsp_impression_hour
        where 
        concat(yr,mt,dt,hh)>='$beg_date' and concat(yr,mt,dt,hh)<='$end_date'
        and ext2='rank_model_camp'
        and countrycode $country_code_condition
		--and get_json_object(ext10,'$.reqtype') in('vin')
		--and os in('android')
		--and exchanges in('inneractive')
    "

    ins_sql="
        select requestid,
        if(split(ext_bp,'\"')[0] is NULL,0.0,split(ext_bp,'\"')[1]) as rev1,
        if(split(ext_bp,'\"')[0] is NULL,0.0,split(ext_bp,'\"')[3]) as rev2,
        1 as ins
        from dwh.ods_adn_trackingnew_install
        where concat(yyyy,mm,dd,hh) between '$beg_date' and '$end_date'
        --and algorithm in ('normal','justcons','justbid','dsp_normal','dsp_just_consume') and publisher_id='6028'
        and parent_session_id in ('rank_model_camp','rank_model_camp_tc')
    "

    sql="
        select
            a.dtm,
            a.exchanges,
			a.adtype,
			--a.countrycode,
			a.os,a.abtag,a.sub_algo,
			--a.campaignid,
			--a.tempid,
			a.srs_strategy,
			sum(a.imp) as imp,
            sum(a.p_ivr) as sum_pivr,
			sum(a.bid_price) as sum_bid_price,
            sum(a.price)/1000 as cost,
            sum(b.rev1) as rev1,
            sum(b.rev2) as rev2,
            sum(b.ins) as ins 
        from(
            ($imp_sql) a left join ($ins_sql) b on a.requestid=b.requestid
        )
        group by
            a.dtm,
            a.exchanges,
			a.adtype,
			--a.countrycode,
			--a.campaignid, 
			--a.tempid,
			a.srs_strategy,
			a.os,a.abtag,a.sub_algo
    ;"

    echo "$sql"
    tag="srs_adx_cpm"
    output="output/${tag}_${beg_date}_${end_date}.txt"
    spark_submit_job "$sql" $tag $output
}

function query_request_data()
{
    beg_date=${1}
    end_date=${2}

    #country_code_condition="in('US')"
    country_code_condition="is not null"

    sql="
        select 
        concat(yr,mt,dt,hh) as day,
        exchanges,countrycode,os,
        get_json_object(ext10,'$.reqtype') as adtype,
        get_json_object(get_json_object(ext3,'$.abtest'),'$.only_saas') as abtag,
        case when get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%' then 'saasAdx' 
            when ext8 like '%,RTDSP%' then 'rtdsp' 
            else 'drs' end as sub_algo,
        --split(split(ext4, ',')[45], '\004')[0] as region,
		--get_json_object(ext3,'$.camp') as ext3_cam,
		--split(get_json_object(ext3,'$.creative'),'#')[3] as tempid,
		\`describe\` as desc_tag,
		round(float(split(split(ext4, ',')[26], '\004')[3]) / float(split(split(ext4, ',')[26], '\004')[4]), 1) as cr_div_cnrr,
        count(1) as request,
		sum(if(float(bid)>0.0,1,0)) as bid_num,
		sum(float(bid)) as bid_sum,
		sum(if(get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%', float(split(ext4, ',')[3]) * 1000, float(get_json_object(ext3,'$.ivr')))) as sum_ivr2,
        sum(ifnull(float(get_json_object(ext3,'$.ivr')),0.0)) as sum_ivr
        from adn_dsp.log_adn_dsp_request_orc_hour
        where 
        concat(yr,mt,dt,hh)>='$beg_date' and concat(yr,mt,dt,hh)<='$end_date'
        and get_json_object(ext3,'$.abtest') is not null
        and countrycode $country_code_condition
		and get_json_object(ext10,'$.reqtype') in('vin')
		--and exchanges in('inneractive')
		--and os in('android')
        group by
        concat(yr,mt,dt,hh),
        exchanges,countrycode,os,
        get_json_object(ext10,'$.reqtype'),
        get_json_object(get_json_object(ext3,'$.abtest'),'$.only_saas'),
        case when get_json_object(ext3,'$.strategy') like '%SaaSAdxModelRanker%' then 'saasAdx' 
            when ext8 like '%,RTDSP%' then 'rtdsp' 
            else 'drs' end,
		round(float(split(split(ext4, ',')[26], '\004')[3]) / float(split(split(ext4, ',')[26], '\004')[4]), 1),
        --split(split(ext4, ',')[45], '\004')[0],
		--split(get_json_object(ext3,'$.creative'),'#')[3],
		\`describe\`
		--get_json_object(ext3,'$.camp')
    "

    echo "$sql"
    tag="adx-req"
    output="output/${tag}_${beg_date}_${end_date}.txt"
    spark_submit_job "$sql" $tag $output
}

query_request_data 2022092700 2022092712 &
#query_adx_cpm 2022092600 2022092723 &
#query_adx_imp 2022082700 2022082823

