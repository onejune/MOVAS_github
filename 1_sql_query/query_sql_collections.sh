export HADOOP_HOME=/data/hadoop-home/hdp-hadoop-3.1.1
export SPARK_HOME=/data2/hadoop-home/spark-3.1.1-bin-free-c59d19df39
export SPARK_CONF_DIR=/data2/hadoop-config/command-home/spark-k8s-offline-development-scientist-a-3.1/conf
export LANG="en_US.UTF-8"

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
    --num-executors 400 \
    --executor-cores 2 \
	--driver-cores 4 \
    --driver-memory 12G \
    --executor-memory 6G \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=500 \
    --conf spark.core.connection.ack.wait.timeout=600 \
    --class org.mobvista.dataplatform.SubmitSparkSql s3://mob-emr/adn/k8s_spark_migrate/release/spark-sql-submitter-1.0.0.jar \
    "${sql}"  \
    "${hive_out}"  \
    "1" \
    Overwrite \
    csv \
	"," \
	true

    hadoop fs -text $hive_out/* > $output
}

#########################################################################
#global variable
unit_ids='201096,198010,206663,198008,175874,187113,198608,175870,221535,165378,206663,198608,227083'
algo_field="
	sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(ext_algo, ',')[23] as flow_type,
    split(ext_algo, ',')[8] as trynew_type,
	split(ext_algo, ',')[36] as if_support_polaris"


#########################################################################

function query_recall_and_timeout() {
    beg_date=$1
    end_date=$2

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"

    sql="select
	ext_reduceFillReq, ext_reduceFillReq,
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
	if(extra2!='', 'fill', 'no_fill') as if_fill,  
	split(split(strategy, '\\;')[7], '-')[1] as strategy,
    split(split(ext_algo, ',')[45], '\004')[0] as region,
    sum(split(strategy, '\\;')[1]) as sum_recalled_cnt,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(ext_algo, ',')[30]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[31]) as sum_ecpm_floor, 
    count(*) as cnt 
    from dwh.ods_adn_trackingnew_request
    where
    concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type,
	ext_reduceFillReq, ext_reduceFillReq,
	if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40],  
	split(split(strategy, '\\;')[7], '-')[1],
	split(split(ext_algo, ',')[45], '\004')[0];"

    hive -e "$sql" >output/recalled_cnt_${beg_date}_${end_date}.txt
}

function query_daily_roi() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
	#if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code, 
	#and ad_type in('rewarded_video','interstitial_video', 'sdk_banner', 'more_offer', 'native')

    unit_ids='498064,1530173,1534519'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
	#country_code_condition="in('us', 'other')"
	country_code_condition="is not null"
    platform_condition="in('android', 'ios')"

	dtm="substr(req_time,1,8)"

	sql="select $dtm as dtm, 
	substr(req_time,1,8) as day,
	ad_type,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other') as country_code,
	is_makemoney_traffic,
	platform,
	sdp_tag,
	ishb,
	bucket,
    sum(total_req) as req, 
	sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as click,
	sum(normal_install) as install,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
    sum(cost)+sum(adx_cost) as total_cost
    from monitor_effect.model_effect_info_test_k8s
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
	and req_time>='$begin_date' and req_time<='$end_date'
	and (bucket like '1%' or bucket like '3%')
	and unit_id $unit_id_condition
	and ad_type in('rewarded_video','interstitial_video') 
	and country_code $country_code_condition
    and platform $platform_condition
	group by 
	substr(req_time,1,8),
	$dtm,
	ad_type,
	ishb,
	bucket,
	platform,
	if(lower(country_code) in('cn','us', 'jp', 'uk'), lower(country_code),'other'),
	sdp_tag,
	is_makemoney_traffic
	;"

    echo "$sql"
    tag="roi_effect"
    output="output/${tag}_${begin_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}

function query_campaign_package() {
	dtm=$1
	sql="select
		id as campaign_id,
		case when ctype=1 then 'CPI'
		when ctype=2 then 'CPC'
		when ctype=3 then 'CPM'
		when ctype=4 then 'CPA'
		when ctype=5 then 'CPE'
		else 'other' end as receivedtype,
		user_activation,
		trace_app_id as package_name
		from
		adn_seesaw.campaign_903
		where
		pdate=$dtm;"
	echo "$sql"
	tag="package_name"
	output="output/campaign_package_mapping.txt"
	spark_submit_job "$sql" $tag $output
}


function query_package_tags(){
	export LANG="en_US.UTF-8"
	sql="select
		package_name,
		first_tag,second_tag,third_tag,
		case 
			when second_tag in ('Hardcore') then '重度'
			when (first_tag not in ('Game','Make-Money-Online')) or (first_tag in ('Make-Money-Online') and second_tag in ('Tools')) then '工具'
			when (second_tag in ('Blackjack Game','Bingo game','Chinese poker','Domino','Party Game','Card','Rummy','Slots','Mahjong','Room Escape','Jigsaw','Chess','Sudoku','Sports Betting Game','Dice','Word Game','Cleanup Game','Real-Money Game','Trivia Quiz Game','Solitaire','Midcore','Board Game')) or (first_tag in ('Make-Money-Online') and second_tag in ('Bingo game','Decryption','Chinese poker','Scratch Cards','Merge Game','Slots','Mahjong','Jigsaw','Solitaire','Coloring Game','Dice','Word Game','Cleanup Game')) then '五垂'
			else '休闲'
		end as tags,
		case 
			when first_tag not in ('Game','Make-Money-Online') and first_tag is not NULL then '工具'
			when first_tag in ('Make-Money-Online') and second_tag in ('Tools') then '工具网赚'
			when first_tag in ('Make-Money-Online') and second_tag in ('Bingo game','Chinese poker','Cleanup Game','Coloring Game','Decryption','Dice','Jigsaw','Mahjong','Merge Game','Scratch Cards','Slots','Solitaire','Word Game','Domino','Board Game','Trivia Quiz Game') then '五垂网赚'
			when first_tag in ('Make-Money-Online') and second_tag in ('Casual') then '休闲网赚'
			when second_tag in ('Hardcore') then '重度'
			when second_tag in ('Midcore') and third_tag in ('Costume game','Palace Game') then '重度女性'
			when ((second_tag in ('Midcore') and third_tag in ('Merge Game','Music Game','Simulation Game')) or second_tag in ('Room Escape') ) then '中度模拟经营'
			when second_tag in ('Real-Money Game','Rummy') then '真金'
			when second_tag in ('Action game','Adventure Game','Arcade Game','Idle','Merge Game','Parkour Game','Racing Game','Role Playing Game','Shooting Game','Simulation Game','Sports Game','Strategy Battle','Tower Defense') then '休闲垂类'
			when second_tag in ('Bingo game','Blackjack Game','Board Game','Card','Chess','Chinese poker','Cleanup Game','Dice','Domino','Jigsaw','Mahjong','Midcore','Party Game','Slots','Solitaire','Sports Betting Game','Sudoku','Trivia Quiz Game','Word Game') then '五垂'
			else '超休闲'
			end as project_tag
		from dwh.dmp_package_tag
		;"
	echo "$sql"
	tag="campaign_tags"
	output="output/package_tags.txt"
	spark_submit_job "$sql" $tag $output
}

function query_daily_roi_with_ad() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_ids='199425'
    unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    
	sql="select concat(year,month,day) as dtm, ad_type, unit_id, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, 
    platform, bucket, campaign_id,
    sum(total_req) as req, sum(impression) as normal_imp, sum(adx_impression) as adx_imp, 
    sum(normal_install) as install, sum(normal_revenue) as normal_rev, sum(adx_revenue) as adx_rev, 
    sum(cost) as normal_cost, sum(adx_cost) as adx_cost
    from monitor_effect.model_effect_info_test 
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
    and req_time>='$begin_date' and req_time<='$end_date'
    and bucket like '1%'
	and unit_id $unit_id_condition
	and campaign_id='223275881'
    group by concat(year,month,day), ad_type, unit_id, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'), 
    platform, bucket, campaign_id;"

    echo $sql
    hive -e "$sql" >output/days_roi_with_campaign_${begin_date}_${end_date}.txt
}

function query_effect_from_tracking_sdk_banner() {
    beg_date=${1}
    end_date=${2}
    record_time=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$record_time=======query for $beg_date - $end_date========"
    output=output/${beg_date}_${end_date}_effect_from_tracking.txt
    echo $record_time

    ins_delay_hours=24
    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('us','cn')"

    imp_sql="select substr(split(ext_algo, ',')[1],1,10) as dtm, campaign_id,
    ad_type, unit_id, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc,
    $algo_field,
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression_merge 
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and strategy like 'MNormalAlpha%' 
    and ad_type in('sdk_banner') 
    and split(ext_algo, ',')[1]>=$beg_date 
    and split(ext_algo, ',')[1]<=$end_date 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by substr(split(ext_algo, ',')[1],1,10), ad_type, unit_id, campaign_id,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[8],split(ext_algo, ',')[23],
    split(split(strategy, '\\;')[7], '-')[1]"

    ins_end_date=$(date -d "+$ins_delay_hours hour ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    echo "ins_end_date:$ins_end_date"

    ins_sql="select substr(split(ext_algo, ',')[1],1,10) as dtm, campaign_id,
    ad_type, unit_id, if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other') as cc, 
    split(ext_algo, ',')[8] as trynew_type,
    split(ext_algo, ',')[23] as flow_type,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1) as idfa,
    count(*) as ins, sum(split(ext_algo, ',')[2]) as rev 
    from dwh.ods_adn_trackingnew_install_merge 
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${ins_end_date} 
    and strategy like 'MNormalAlpha%' and ad_type in('rewarded_video','interstitial_video', 'sdk_banner') 
    and split(ext_algo, ',')[1]>=$beg_date and split(ext_algo, ',')[1]<=$end_date
    and (unix_timestamp(concat(yyyy,mm,dd,hh), 'yyyyMMddHH')-unix_timestamp(split(ext_algo, ',')[1], 'yyyyMMddHH'))/3600<=$ins_delay_hours  
    group by substr(split(ext_algo, ',')[1],1,10), ad_type, unit_id, campaign_id,
    if(idfa='00000000-0000-0000-0000-000000000000',0,1),
    if(lower(country_code) in('us','cn','jp','uk'),lower(country_code),'other'), 
    split(ext_algo, ',')[8],split(ext_algo, ',')[23],
    split(split(strategy, '\\;')[7], '-')[1]"

    sql="select a.*, b.ins, b.rev 
    from($imp_sql) a left join($ins_sql) b 
    on a.dtm=b.dtm 
    and a.ad_type=b.ad_type and a.cc=b.cc and a.unit_id=b.unit_id and a.trynew_type=b.trynew_type and a.flow_type=b.flow_type
    and a.campaign_id=b.campaign_id
    and a.idfa=b.idfa;"

    echo "sql:$sql"

    hive -e "$sql" >$output
}

function query_request_campaign_sdk_banner() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition="in($unit_ids)"
    unit_id_condition='is not null'
    country_code_condition="in('us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
    if(lower(country_code) in('us'),lower(country_code),'other') as country_code, 
    split(ext_algo, ',')[23] as flow_type, app_id,
    unit_id, platform,
	split(ext_algo, ',')[0] as algo_cam,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_ration, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_impression_merge 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%' and ad_type in('sdk_banner')
    group by 
	concat(yyyy,mm,dd), ad_type, app_id, unit_id, platform, if(lower(country_code) in('us'), lower(country_code),'other'), 
    split(ext_algo, ',')[40],  
    split(ext_algo, ',')[23],
	split(ext_algo, ',')[0]
    ;"

    echo $sql
    hive -e "$sql" >output/impression_sdk_banner_${beg_date}_${end_date}.txt
}


function query_effect_from_tracking_with_campaign() {
    beg_date=${1}
    end_date=${2}

    unit_ids='407868,402733,389620'
    publisher_ids='10714'
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="is not null"
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, app_id,
	unit_id, campaign_id,
	split(strategy, '\\;')[5] as is_make_money_offer,
	split(strategy, '\\;')[0] as ranker,
	split(strategy, '\\;')[6] as group,
	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
	split(ext_algo, ',')[14] as tpl_group,
	split(ext_algo, ',')[15] as video_tpl,
    split(ext_algo, ',')[3] as cr_ivr,
    split(ext_algo, ',')[4] as cr_cpm,
	split(split(ext_algo, ',')[24], '\004')[0] as imp_ration,
	split(split(ext_algo, ',')[25], '\004')[1] as fr_cpm,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	get_json_object(ext_dsp,'$.price_out') as hb_cost,
	split(ext_algo, ',')[8] as trynew_type,
	split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(split(ext_algo, ',')[20], '\004')[1] as video_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] as playable_trynew,
	split(ext_algo, ',')[2] as price
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video', 'sdk_banner')
    "

    sql="select dtm, app_id, unit_id, country_code, ad_type,
	campaign_id,
	platform, is_hb,
	is_make_money_offer, ranker, group,
	count(requestid) as imp, sum(cr_cpm) as cr_cpm, sum(fr_cpm) as fr_cpm, sum(cr_ivr) as cr_ivr, 
	sum(rev) as rev, sum(ins) as ins,
	sum(hb_cost) as hb_cost
    from (select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id, a.app_id as app_id,
	a.dtm as dtm, a.ad_type as ad_type, a.imp_ration as imp_ration, a.price as price, a.trynew_type as trynew_type,
	a.tpl_group as tpl_group, a.video_tpl as video_tpl,
	a.tpl_trynew as tpl_trynew,
	a.video_trynew as video_trynew,
	a.unit_id as unit_id, a.campaign_id as campaign_id,
	a.is_make_money_offer as is_make_money_offer,
	a.ranker as ranker,
	a.group as group,
	a.playable_trynew as playable_trynew,
	a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.cr_cpm as cr_cpm, a.cr_ivr as cr_ivr, a.fr_cpm as fr_cpm,
    b.rev as rev, b.ins as ins, a.is_hb as is_hb, a.hb_cost as hb_cost
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid)c
    group by dtm, ad_type, app_id, country_code, platform, is_hb,
	unit_id, campaign_id,
	is_make_money_offer, ranker, group
	having imp>0;"

    #sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
    #from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

    echo "sql:$sql"
	tag="cpm_effect"
    output="output/${beg_date}_${end_date}_effect_from_tracking.txt"
	spark_submit_job "$sql" $tag $output
}

function query_effect_from_tracking_with_crt_cnt() {
    beg_date=${1}
    end_date=${2}

    unit_ids="'384423','359828','391295','310326','366820', '369750', '378309'"
    output=output/${beg_date}_${end_date}_effect_from_tracking_with_crt_cnt.txt
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"

    imp_sql="select ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm,
    lower(country_code) as country_code, publisher_id,
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name, platform,
    split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
	split(split(ext_algo, ',')[19], '\004')[1] as 106_cnt,
    split(split(ext_algo, ',')[19], '\004')[2] as 201_cnt,
    split(split(ext_algo, ',')[19], '\004')[3] as 61002_cnt,
    split(split(ext_algo, ',')[19], '\004')[7] as tpl_cnt,
	get_json_object(ext_dsp,'$.is_hb') as is_hb
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner') 
	and strategy like '%1_cnrr2%'
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner')
    "

    sql="select dtm, placement_id, publisher_id, country_code, ad_type, strategy_name, platform, is_hb,
    106_cnt,201_cnt,61002_cnt,tpl_cnt,
    count(requestid) as imp, sum(max_fr) as max_fr, sum(rev) as rev 
    from (select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id,
	a.dtm as dtm, a.ad_type as ad_type,
	a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
    b.rev as rev, a.is_hb as is_hb,
    a.106_cnt as 106_cnt,
    a.201_cnt as 201_cnt,
    a.61002_cnt as 61002_cnt,
    a.tpl_cnt as tpl_cnt
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=24*3600)c
    group by dtm, placement_id, ad_type, publisher_id, country_code, strategy_name, platform, is_hb,
    106_cnt,201_cnt,61002_cnt,tpl_cnt;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

function query_effect_from_tracking_with_trynew() {
    beg_date=${1}
    end_date=${2}

    unit_ids='407868,402733,389620'
    publisher_ids='10714'
    output="output/${beg_date}_${end_date}_trynew_effect_from_tracking.txt"
    unit_id_condition='is not null'
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"
    #publisher_id_condition="in($publisher_ids)"
    publisher_id_condition='is not null'

    imp_sql="select platform, ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, lower(country_code) as country_code, publisher_id, 
	split(split(strategy, '\\;')[7], '-')[1] as strategy_name,
    split(ext_algo, ',')[3] as cr_ivr,
    split(ext_algo, ',')[4] as cr_cpm,
	split(split(ext_algo, ',')[24], '\004')[0] as imp_ration,
	split(split(ext_algo, ',')[25], '\004')[1] as fr_cpm,
	get_json_object(ext_dsp,'$.is_hb') as is_hb,
	split(ext_algo, ',')[8] as trynew_type,
	split(split(ext_algo, ',')[20], '\004')[0] as tpl_trynew,
	split(split(ext_algo, ',')[20], '\004')[1] as video_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] as playable_trynew,
	split(ext_algo, ',')[2] as price
    from dwh.ods_adn_trackingnew_impression_merge
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video', 'sdk_banner') 
    and unit_id $unit_id_condition
	and publisher_id $publisher_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev,
	1 as ins
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video', 'sdk_banner')
    "

    sql="select dtm, publisher_id, country_code, ad_type, strategy_name, platform, is_hb, trynew_type,
	tpl_trynew, video_trynew, playable_trynew,
	count(requestid) as imp, sum(cr_cpm) as cr_cpm, sum(fr_cpm) as fr_cpm, sum(cr_ivr) as cr_ivr, sum(rev) as rev, sum(ins) as ins,
	sum(imp_ration) as imp_ration
    from (select a.requestid as requestid, a.publisher_id as publisher_id,
	a.dtm as dtm, a.ad_type as ad_type, a.imp_ration as imp_ration, a.price as price, a.trynew_type as trynew_type,
	a.tpl_trynew as tpl_trynew,
	a.video_trynew as video_trynew,
	a.playable_trynew as playable_trynew,
	a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.cr_cpm as cr_cpm, a.cr_ivr as cr_ivr, a.fr_cpm as fr_cpm,
    b.rev as rev, b.ins as ins, a.is_hb as is_hb
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=24*3600)c
    group by dtm, ad_type, publisher_id, country_code, strategy_name, platform, is_hb, trynew_type,
	tpl_trynew,playable_trynew,video_trynew
	having imp>0;"

    #sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
    #from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

function query_effect_from_tracking_with_campaign_creative() {
    beg_date=${1}
    end_date=${2}

    unit_ids='359828'
    output=output/${beg_date}_${end_date}_effect_from_tracking_with_campaign_creative.txt
    #unit_id_condition='is not null'
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn','us')"
    udf_jar="s3://mob-emr-test/wanjun/udf/udf_creative_parser.jar"

    imp_sql="
    select ext_placementId, ad_type, requestid, created, concat(yyyy,mm,dd) as dtm, unit_id, lower(country_code) as country_code, publisher_id, app_id,
	split(split(strategy, '\\;')[7], '-')[0] as strategy_name, platform,campaign_id,split(ext_algo, ',')[8] as trynew_type,
	split(split(ext_algo, ',')[20], '\004')[1] as img_trynew,
	split(split(ext_algo, ',')[20], '\004')[2] as video_trynew,
	split(split(ext_algo, ',')[20], '\004')[3] as playable_trynew,
    split(split(get_crt_str(ext_stats), '#')[0], ';')[1] as crt_201_id,
    split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
    get_json_object(ext_dsp,'$.is_hb') as is_hb
    from dwh.ods_adn_trackingnew_impression_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video') 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition"

    ins_sql="select ext_placementId, requestid, created,
    split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video')
    "

    sql="
	add jar $udf_jar;
	CREATE TEMPORARY FUNCTION get_crt_str AS 'com.mobvista.udf.creative.GetCrtStr';
	select dtm, placement_id, publisher_id, app_id, unit_id, country_code, ad_type, strategy_name, platform, is_hb,campaign_id, crt_201_id, trynew_type,
	img_trynew, video_trynew, playable_trynew,
    count(requestid) as imp, sum(max_fr) as max_fr, sum(rev) as rev 
    from (select a.ext_placementId as placement_id, a.requestid as requestid, a.publisher_id as publisher_id, a.app_id as app_id,
    a.dtm as dtm, a.ad_type as ad_type, a.unit_id as unit_id, a.campaign_id as campaign_id,a.crt_201_id as crt_201_id,a.trynew_type as trynew_type,
    a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
    b.rev as rev, a.is_hb as is_hb,
	a.img_trynew as img_trynew, a.video_trynew as video_trynew, a.playable_trynew as playable_trynew
    from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=3*3600)c
    group by dtm, placement_id, ad_type, publisher_id, app_id, unit_id, country_code, strategy_name, platform, is_hb, campaign_id, crt_201_id, trynew_type,
	img_trynew, video_trynew, playable_trynew;"

    echo "sql:$sql"
    hive -e "$sql" >$output
}

function query_rev_by_cam() {
	beg_date=$1
	end_date=$2

	sql="select 
		concat(yyyy,mm,dd) as dtm,
		unit_id, campaign_id,
		if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code,
		platform, ad_type,
		split(strategy, '\\;')[5] as is_make_money_offer,
		split(strategy, '\\;')[0] as ranker,
		split(strategy, '\\;')[6] as group,
		get_json_object(ext_dsp,'$.is_hb') as is_hb,
		sum(split(ext_algo, ',')[2]) as rev,
		count(*) as ins
		from dwh.ods_adn_trackingnew_install
		where 
		concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
		and strategy like '%MNormalAlpha%'
		group by
		platform, ad_type, concat(yyyy,mm,dd), 
		if(lower(country_code) in('cn','us'),lower(country_code),'other'),
		unit_id, campaign_id,
		split(strategy, '\\;')[5],
		split(strategy, '\\;')[0],
		split(strategy, '\\;')[6],
		get_json_object(ext_dsp,'$.is_hb')
		"
		echo "$sql"
		tag="rev_effect"
		output="output/${tag}_${beg_date}_${end_date}.txt"
		spark_submit_job "$sql" $tag $output
}

function query() {
	sql="select 
		concat(yyyy,mm,dd) as dtm,
		if(get_json_object(ext_bp,'$.[0]')!=0, get_json_object(ext_bp,'$.[0]'), split(ext_algo, ',')[2]) as rev1,
		get_json_object(ext_bp,'$.[0]') as rev2,
		get_json_object(ext_dsp,'$.price_out')/1000/100 as hb_cost
		from dwh.ods_adn_trackingnew_impression
		where 
		strategy like '%MNormalAlpha%'
		and get_json_object(ext_dsp,'$.is_hb')='1'
		and concat(yyyy,mm,dd,hh) between '2022041712' and '2022041712'
		and ad_type in('rewarded_video','interstitial_video')
	"
	echo "$sql"
	tag="query_t"
	output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}


function query_ici_from_tracking() {
	beg_date=$1
	end_date=$2

	key="concat_ws('\t',concat(yyyy,mm,dd,hh), unit_id, country_code, ad_type, platform, get_json_object(ext_dsp,'$.is_hb'), split(strategy, '\\;')[0], ext_campaignpackagename)"
	key="concat_ws('\t',concat(yyyy,mm,dd), ad_type, platform, get_json_object(ext_dsp,'$.is_hb'), split(strategy, '\\;')[0], ext_campaignpackagename)"
	#key="concat_ws('\t',concat(yyyy,mm,dd), unit_id, country_code)"

	imp_sql="select 
		$key as key,
		count (*) as imp,
		sum(get_json_object(ext_dsp,'$.price_out')/1000/100) as cost
		from dwh.ods_adn_trackingnew_impression
		where (concat(yyyy,mm,dd,hh) >= "${beg_date}" and concat(yyyy,mm,dd,hh) <= "${end_date}")
		and strategy like '%MNormalAlpha%' 
		and ad_type in('rewarded_video','interstitial_video')
		group by
		$key"
	clk_sql="select 
		$key as key,
		count (*) as clk
		from dwh.ods_adn_trackingnew_click
		where (concat(yyyy,mm,dd,hh) >= "${beg_date}" and concat(yyyy,mm,dd,hh) <= "${end_date}")
		and strategy like '%MNormalAlpha%'  
		group by
		$key"
	ins_sql="select 
		$key as key,
		count (*) as ins,
		sum(if(get_json_object(ext_bp,'$.[0]')!=0, get_json_object(ext_bp,'$.[0]'),split(ext_algo, ',')[2])) as rev
		from dwh.ods_adn_trackingnew_install
		where (concat(yyyy,mm,dd,hh) >= "${beg_date}" and concat(yyyy,mm,dd,hh) <= "${end_date}")
		and strategy like '%MNormalAlpha%' 
		group by
		$key"

	sql="select 
			a.key,
			a.imp, 
			b.clk, 
			c.ins,
			c.rev,
			a.cost
		from 
			($imp_sql) a 
			left join ($clk_sql) b on a.key = b.key
			left join ($ins_sql) c on a.key = c.key
		;"	
	echo "$sql"
	tag="query_ici_a"
	output="output/${tag}_${beg_date}_${end_date}.txt"
	spark_submit_job "$sql" $tag $output
}

#query_recall_and_timeout 2020020100 2020030100 &
#query_effect_from_tracking_with_crt_cnt 2021012920 2021020123 &
#query_effect_from_tracking_with_trynew 2021012920 2021020123 &
#query_request_campaign_sdk_banner 2020082400 2020082900 &
query_daily_roi 2022070600 2022070823 

#query_effect_from_tracking_with_campaign 2022041400 2022042323

#query_rev_by_cam 2022041700 2022042023

#query_ici_from_tracking 2022041000 2022042823

#query_campaign_package 20220418
#query_package_tags


#query
