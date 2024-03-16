function query_bidrate_roi() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi
	ins_delay_hours=24
	unit_id_condition='in(213848)'
	unit_id_condition='is not null'
	country_code_condition="in('us')"

	#get win/loss of each token
	hb_event="select concat(yyyy,mm,dd) as dtm, publisher_id, app_id, unit_id, dsp_id,
	split(extra3, '-')[3] as strategy_name,
	(price/100/1000) as bidprice,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, city_code, ad_type, platform, 
	split(token, '_')[0] as token, 
	split(ext_adx_algo,'\004')[2] as optimized_bidprice,
	split(ext_adx_algo,'\004')[3] as predicted_bidprice,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
	split(split(ext_algo, ',')[28], '\004')[0] as algorithm,
	(case event_type when '1' then 'win' else 'loss' end) as event_type
	from dwh.ods_adn_hb_v1_event 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video')
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	#get price_out of each token in order to figure cost
	adx_imp="select concat(yyyy,mm,dd) as dtm, request_id, ch_request_id,
	(price/100/1000) as price_out, (dsp_price_in/100/1000) as price_raw  
	from dwh.ods_adn_adx_tracking_v1_impression 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and is_hb='1'
	and ch_spot_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	#get revenue of each request in order to figure revenue
	ins_end_dtm=$(date -d "+$ins_delay_hours hour ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
	echo "ins_end_date:$ins_end_dtm"

	adn_ins="select concat(yyyy,mm,dd) as dtm, requestid, 
	if(get_json_object(ext_bp,'$.[0]')!=0, get_json_object(ext_bp,'$.[0]'),split(ext_algo, ',')[2]) as price
	from dwh.ods_adn_trackingnew_install_merge
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$ins_end_dtm
	and split(ext_algo, ',')[1]>=$begin_date and split(ext_algo, ',')[1]<=$end_date 
	and (unix_timestamp(concat(yyyy,mm,dd,hh), 'yyyyMMddHH')-unix_timestamp(split(ext_algo, ',')[1], 'yyyyMMddHH'))/3600<=$ins_delay_hours
	and ad_type in('rewarded_video','interstitial_video')"

	#hb_event join adx_imp join adn_ins
	sql="select event.dtm, event.event_type, event.ad_type, event.unit_id, event.country_code, event.platform, event.dsp_id, event.strategy_name,
	event.algorithm,
	sum(event.bidprice) as sum_bidprice,
	sum(event.optimized_bidprice) as sum_optimized_bidprice,
	sum(event.predicted_bidprice) as sum_predicted_bidprice,
	sum(event.max_fillrate_score) as sum_max_fillrate_score,
	count(DISTINCT event.token) as req, count(DISTINCT imp.request_id) as imp, 
	sum(imp.price_out) as cost, sum(imp.price_raw) as price_raw, sum(ins.price) as revenue
	from ($hb_event) event 
	left join ($adx_imp) imp on event.token=imp.request_id
	left join ($adn_ins) ins on imp.ch_request_id=ins.requestid
	group by event.dtm, event.event_type, event.ad_type, event.unit_id, event.country_code, event.platform, event.dsp_id, event.strategy_name,
	event.algorithm
	;"

	echo "$sql"
	hive -e "$sql" >output/hb_roi_${begin_date}_${end_date}.txt
}

function query_bidrate_cost() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi

	unit_id_condition='is not null'
	country_code_condition="in('us')"

	#get win/loss of each token
	hb_event="select concat(yyyy,mm,dd) as dtm, publisher_id, app_id, unit_id, dsp_id,
	split(extra3, '-')[3] as strategy_name,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, city_code, ad_type, platform,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(token, '_')[0] as token, 
	(price/100/1000) as bidprice,
	split(ext_adx_algo,'\004')[2] as optimized_bidprice,
	split(ext_adx_algo,'\004')[3] as predicted_bidprice,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
	split(split(ext_algo, ',')[28], '\004')[0] as algorithm,
	(case event_type when '1' then 'win' else 'loss' end) as event_type
	from dwh.ods_adn_hb_v1_event 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video')
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	#get price_out of each token in order to figure cost
	adx_imp="select concat(yyyy,mm,dd) as dtm, request_id, ch_request_id,
	(price/100/1000) as price_out, (dsp_price_in/100/1000) as price_raw  
	from dwh.ods_adn_adx_tracking_v1_impression 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and is_hb='1'
	and ch_spot_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	sql="select event.dtm, event.event_type, event.ad_type, event.unit_id, event.country_code, event.platform, event.dsp_id, event.strategy_name,
	event.calibration,
	sum(event.bidprice) as sum_bidprice,
	sum(event.optimized_bidprice) as sum_optimized_bidprice,
	sum(event.predicted_bidprice) as sum_predicted_bidprice,
	sum(event.max_fillrate_score) as sum_max_fillrate_score,
	count(DISTINCT event.token) as req, count(DISTINCT imp.request_id) as imp, 
	sum(imp.price_out) as cost, sum(imp.price_raw) as price_raw
	from ($hb_event) event 
	left join ($adx_imp) imp on event.token=imp.request_id
	group by event.dtm, event.calibration, event.event_type, event.ad_type, event.unit_id, event.country_code, 
	event.platform, event.dsp_id, event.strategy_name
	;"

	echo "$sql"
	hive -e "$sql" >output/hb_cost_${begin_date}_${end_date}.txt
}

function query_detailed_info_for_hb() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi
	unit_ids='244112,198608,206663,293668'
	unit_id_condition="in($unit_ids)"
	country_code_condition="in('us')"

	#get win/loss of each token
	hb_event="select concat(yyyy,mm,dd) as dtm, publisher_id, app_id, unit_id, dsp_id,
	split(extra3, '-')[3] as strategy_name,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, ad_type, platform, 
	split(token, '_')[0] as token, 
	(price/100/1000) as bidprice,
	split(ext_adx_algo,'\004')[2] as optimized_bidprice,
	split(ext_adx_algo,'\004')[3] as predicted_bidprice,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
	split(split(ext_algo, ',')[28], '\004')[2] as wa,
	split(split(ext_algo, ',')[28], '\004')[3] as wb,
	split(split(ext_algo, ',')[28], '\004')[4] as opt_bidprice,
	split(split(ext_algo, ',')[28], '\004')[7] as is_calibration,
	split(split(ext_algo, ',')[28], '\004')[8] as promote_ratio,
	(case event_type when '1' then 'win' else 'loss' end) as event_type
	from dwh.ods_adn_hb_v1_event 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video')
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	#get price_out of each token in order to figure cost
	adx_imp="select concat(yyyy,mm,dd) as dtm, request_id, ch_request_id,
	(price/100/1000) as price_out, (dsp_price_in/100/1000) as price_raw  
	from dwh.ods_adn_adx_tracking_v1_impression 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and is_hb='1'
	and ch_spot_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	sql="select event.*, imp.price_out, imp.price_raw
	from($hb_event) event 
	left join ($adx_imp) imp on event.token=imp.request_id
	;"

	echo "$sql"
	hive -e "$sql" >output/hb_detailed_${begin_date}_${end_date}.txt
}

function query_hb_cpm_predict() {
	beg_date=${1}
	end_date=${2}
	record_time=$(date +"%Y-%m-%d %H:%M:%S")
	echo "$record_time=======query for $beg_date - $end_date========"
	output=output/${beg_date}_${end_date}_hb_predict_and_cpm.txt
	echo $record_time

	ins_delay_hours=24
	unit_id_condition="not null"
	country_code_condition="in('us')"

	imp_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, 
	split(split(strategy, '\\;')[7], '-')[3] as strategy_name, 
    ad_type, unit_id, if(lower(country_code) in('cn','us'),lower(country_code),'other') as cc,
    split(ext_algo, ',')[23] as flow_type, 
	campaign_id, 
	get_json_object(ext_dsp,'$.dspid') as dsp_id,
    sum(split(split(ext_algo, ',')[26], '\004')[2]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[2]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0])/1000 as max_fillrate_score, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    sum(split(split(ext_algo, ',')[28], '\004')[4])/1000 as sum_optimized_bidprice,
	sum(get_json_object(ext_dsp,'$.price_out'))/1000/100 as cost,
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression_merge
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video', 'banner') 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
	and get_json_object(ext_dsp,'$.is_hb')=1
    group by substr(split(ext_algo, ',')[1],1,8), ad_type, unit_id, campaign_id,
    split(ext_algo, ',')[23],if(lower(country_code) in('cn','us'), lower(country_code),'other'), 
    split(split(strategy, '\\;')[7], '-')[3],
	get_json_object(ext_dsp,'$.dspid')"

	ins_end_date=$(date -d "+$ins_delay_hours hour ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
	echo "ins_end_date:$ins_end_date"

	ins_sql="select substr(split(ext_algo, ',')[1],1,8) as dtm, 
	split(split(strategy, '\\;')[7], '-')[3] as strategy_name, 
    ad_type, unit_id, if(lower(country_code) in('cn','us'),lower(country_code),'other') as cc, 
    split(ext_algo, ',')[23] as flow_type,
	campaign_id, 
	get_json_object(ext_dsp,'$.dspid') as dsp_id,
    count(*) as ins, 
	sum(split(ext_algo, ',')[2]) as rev 
    from dwh.ods_adn_trackingnew_install_merge 
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${ins_end_date} 
    and ad_type in('rewarded_video','interstitial_video', 'banner') 
	and get_json_object(ext_dsp,'$.is_hb')=1
    group by substr(split(ext_algo, ',')[1],1,8), ad_type, unit_id,if(lower(country_code) in('cn','us'),
    lower(country_code),'other'), campaign_id,
    split(ext_algo, ',')[23], split(split(strategy, '\\;')[7], '-')[3],
	get_json_object(ext_dsp,'$.dspid')"

	sql="select a.*, b.ins, b.rev 
    from($imp_sql) a left join($ins_sql) b 
    on a.dtm=b.dtm 
	and a.strategy_name=b.strategy_name
    and a.ad_type=b.ad_type and a.cc=b.cc and a.unit_id=b.unit_id
    and a.flow_type=b.flow_type and a.dsp_id=b.dsp_id and a.campaign_id=b.campaign_id;"

	echo "sql:$sql"

	hive -e "$sql" >$output
}

function query_hb_effect_from_tracking() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_hb_effect_from_tracking.txt
	exclude_units=$(cat hb_factor_big)
	unit_id_condition='in(206663,198608,272281,269332)'
	unit_id_condition='is not null'
	country_code_condition="is not null"
	ins_ret_time=24

	imp_sql="select campaign_id, requestid, created, concat(yyyy,mm,dd) as dtm, app_id, unit_id, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, platform, get_json_object(
	ext_dsp,'$.price_out')/1000/100 as cost, get_json_object(ext_dsp,'$.dspid') as dsp_id,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
	split(ext_algo, ',')[35] as hb_layer
	from dwh.ods_adn_trackingnew_impression where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and ad_type in('rewarded_video','interstitial_video','sdk_banner') 
	and get_json_object(ext_dsp,'$.is_hb')=1
	and unit_id $unit_id_condition
	and unit_id not in($exclude_units)
	and lower(country_code) $country_code_condition"

	ins_sql="select campaign_id, requestid, created,
	split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner')
	"

	sql="select dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id,calibration, ad_type,hb_layer,
	count(requestid) as imp, sum(cost) as cost, sum(max_fr) as max_fr, sum(rev) as rev 
    from 
	(select a.campaign_id, a.requestid as requestid, a.dtm as dtm, a.app_id as app_id, a.unit_id as unit_id, a.ad_type as ad_type, a.hb_layer as hb_layer,
	a.calibration as calibration, a.country_code as country_code, a.dsp_id as dsp_id, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
	a.cost as cost, b.rev as rev
	from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=$ins_ret_time*3600)c
	group by 
	dtm, app_id, hb_layer, unit_id, country_code, strategy_name, platform, dsp_id, calibration, 
	ad_type;"

	#sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
	#from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

	echo "sql:$sql"
	hive -e "$sql" >$output
}

function query_hb_effect_from_tracking_with_campaign() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_hb_effect_from_tracking_with_campaign.txt
	unit_id_condition='in(339986, 333659, 342674, 240301)'
	#unit_id_condition='is not null'
	country_code_condition="is not null"
	ins_ret_time=24

	imp_sql="select campaign_id, requestid, created, concat(yyyy,mm,dd) as dtm, app_id, unit_id, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, platform, get_json_object(
	ext_dsp,'$.price_out')/1000/100 as cost, get_json_object(ext_dsp,'$.dspid') as dsp_id,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
	split(ext_algo, ',')[35] as hb_layer
	from dwh.ods_adn_trackingnew_impression where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and ad_type in('rewarded_video','interstitial_video','sdk_banner') 
	and get_json_object(ext_dsp,'$.is_hb')=1
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	ins_sql="select campaign_id, requestid, created,
	split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner')
	"

	sql="select dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id,calibration, ad_type,hb_layer,
	campaign_id,
	count(requestid) as imp, sum(cost) as cost, sum(max_fr) as max_fr, sum(rev) as rev 
    from 
	(select a.campaign_id, a.requestid as requestid, a.dtm as dtm, a.app_id as app_id, a.unit_id as unit_id, a.ad_type as ad_type, a.hb_layer as hb_layer,
	a.calibration as calibration, a.country_code as country_code, a.dsp_id as dsp_id, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
	a.cost as cost, b.rev as rev
	from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=$ins_ret_time*3600)c
	group by 
	dtm, app_id, hb_layer, unit_id, country_code, strategy_name, platform, dsp_id, calibration, 
	campaign_id,
	ad_type;"

	#sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
	#from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

	echo "sql:$sql"
	hive -e "$sql" >$output
}

function query_hb_houly_cpm_detail_with_campaign() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_hb_hourly_effect.txt
	unit_id_condition='in(206663,198608,272281,269332)'
	#unit_id_condition='is not null'
	country_code_condition="is not null"

	imp_sql="select campaign_id, requestid, created, concat(yyyy,mm,dd,hh) as dtm, unit_id, country_code, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, platform, get_json_object(
	ext_dsp,'$.price_out')/1000/100 as cost, get_json_object(ext_dsp,'$.dspid') as dsp_id,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fr
	from dwh.ods_adn_trackingnew_impression where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and ad_type in('rewarded_video','interstitial_video') 
	and get_json_object(ext_dsp,'$.is_hb')=1
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	ins_sql="select campaign_id, requestid, created,
	split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video')
	"

	sql="select dtm, unit_id, country_code, strategy_name, platform, dsp_id, campaign_id,
	count(requestid) as imp, sum(cost) as cost, sum(max_fr) as max_fr, sum(rev) as rev 
    from (select a.requestid as requestid, a.dtm as dtm, a.unit_id as unit_id, a.dsp_id as dsp_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
	a.cost as cost, b.rev as rev, a.campaign_id
	from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=48*3600)c
	group by dtm, unit_id, country_code, strategy_name, campaign_id, platform, dsp_id;"

	#sql="select a.dtm as dtm, a.unit_id as unit_id, a.country_code as country_code, a.strategy_name as strategy_name, a.platform as platform, b.rev as rev
	#from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid;"

	echo "sql:$sql"
	hive -e "$sql" >$output
}
function query_hb_from_impression() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi

	unit_id_condition="is not null"
	country_code_condition="is not null"

	sql="select 
    substr(split(ext_algo, ',')[1],1,8) as dtm, split(ext_algo, ',')[18] as rs_flag, unit_id, ad_type, platform, 
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    split(split(ext_algo, ',')[28], '\004')[0] as algorithm,
    split(split(ext_algo, ',')[28], '\004')[1] as eta,
    split(split(strategy, '\\;')[7], '-')[3] as strategy_name, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(ext_algo, ',')[30]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[31]) as sum_ecpm_floor,
    sum(split(ext_algo, ',')[2]) as sum_price,
    sum(split(split(ext_algo, ',')[28], '\004')[4]) as sum_optimized_bidprice,
	sum(split(split(ext_algo, ',')[24], '\004')[3]) as sum_frequence_ratio,
    count(*) as imp 
    from dwh.ods_adn_trackingnew_impression_merge 
    where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date
    and split(ext_algo, ',')[1]>=$begin_date and split(ext_algo, ',')[1]<=$end_date
    and strategy like 'MNormalAlpha%'
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and split(ext_algo, ',')[23] like '%-hb%'
    group by unit_id, substr(split(ext_algo, ',')[1],1,8), split(ext_algo, ',')[18], ad_type, platform, if(lower(country_code) in('cn','us'),lower(country_code),'other'), 
    split(split(strategy, '\\;')[7], '-')[3],
    split(split(ext_algo, ',')[28], '\004')[0],
    split(split(ext_algo, ',')[28], '\004')[1];"

	hive -e "$sql" >output/hb_data_${begin_date}_${end_date}.txt
}

function query_hb_effect() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi

	sql="select concat(year,month,day) as dtm, ad_type, unit_id, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, 
	platform, bucket, is_hb, 
	sum(total_req) as req, sum(impression) as normal_imp, sum(adx_impression) as adx_imp, 
	sum(normal_install) as install, sum(normal_revenue) as normal_rev, sum(adx_revenue) as adx_rev, 
	sum(cost) as normal_cost, sum(adx_cost) as adx_cost
	from monitor_effect.model_effect_info_test 
	where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
	and req_time>='$begin_date' and req_time<='$end_date'
	and bucket like '3%'
	and is_hb=1
	group by concat(year,month,day), ad_type, unit_id, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'), 
	platform, bucket, is_hb;"

	echo $sql
	hive -e "$sql" >output/hb_effect_${begin_date}_${end_date}.txt
}

function query_hb_day_day_effect() {
	beg_date=${1}
	end_date=${2}
	beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
	end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
	beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
	end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

	if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:beg_date or end_date is invalid."
		exit
	fi

	sql="select concat(year,month,day) as dtm, ad_type, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code,
    platform, bucket, is_hb,
    sum(total_req) as req, sum(impression) as normal_imp, sum(adx_impression) as adx_imp,
    sum(normal_install) as install, sum(normal_revenue) as normal_rev, sum(adx_revenue) as adx_rev,
    sum(cost) as normal_cost, sum(adx_cost) as adx_cost
    from monitor_effect.model_effect_info_test
    where
    ((concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date) 
    or (concat(year,month,day,hour)>=$beg_date1 and concat(year,month,day,hour)<=$end_date1)
    or (concat(year,month,day,hour)>=$beg_date2 and concat(year,month,day,hour)<=$end_date2)) 
    and(bucket like '3%' or bucket like '1%')
    group by concat(year,month,day), ad_type, if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'),
    platform, bucket, is_hb;"

	echo $sql
	hive -e "$sql" >output/hb_day_day_effect_${begin_date}_${end_date}.txt
}

function query_hb_moreoffer_effect_from_tracking() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_moreoffer_effect.txt
	#unit_id_condition='in(206663,198608,272281,269332)'
	unit_id_condition='is not null'
	country_code_condition="is not null"

	imp_sql="select ext_adxrequestid, concat(yyyy,mm,dd) as dtm, unit_id, lower(country_code) as country_code, campaign_id, ad_type, platform, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, get_json_object(ext_dsp,'$.dspid') as dsp_id, get_json_object(ext_dsp,'$.is_hb') as is_hb
	from dwh.ods_adn_trackingnew_impression_merge 
    where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
    and ad_type in('rewarded_video','interstitial_video') 
    and lower(country_code) $country_code_condition
	and get_json_object(ext_dsp,'$.is_hb')=1"

	ins_sql="select unit_id, ad_type, get_json_object(ext_data2,'$.crt_rid') as rviv_rid, 
	if(get_json_object(ext_bp,'$.[0]')!=0, get_json_object(ext_bp,'$.[0]'),split(ext_algo, ',')[2]) as rev, campaign_id
    from dwh.ods_adn_trackingnew_install 
    where concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date 
    and ad_type like 'more_offer'"

	sql="select dtm, unit_id, country_code, strategy_name, platform, dsp_id, count(request_id) as imp, 'more_offer' as ad_type, '0' as cost, '0' as max_fr, sum(rev) as rev 
    from(select a.ext_adxrequestid as request_id, a.is_hb as is_hb, a.country_code as country_code, a.dtm as dtm, a.campaign_id as campaign_id, 
	a.unit_id as unit_id, a.platform as platform, a.ad_type as ad_type, a.strategy_name as strategy_name, a.dsp_id as dsp_id, b.rev as rev 
    from($imp_sql)a inner join ($ins_sql)b on a.ext_adxrequestid=b.rviv_rid)c 
    group by dtm, unit_id, country_code, strategy_name, platform, dsp_id;"

	echo "$sql"
	hive -e "$sql" >$output
}

function query_hb_price_compare_from_tracking() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_hb_price_compare_from_tracking.txt
	unit_id_condition='in(206663,198608,272281,269332)'
	#unit_id_condition='is not null'
	country_code_condition="is not null"
	ins_ret_time=48

	imp_sql="select campaign_id, requestid, created, concat(yyyy,mm,dd) as dtm, app_id, unit_id, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, platform, 
    get_json_object(ext_dsp,'$.price_out')/1000/100 as cost, get_json_object(ext_dsp,'$.dspid') as dsp_id,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(split(ext_algo, ',')[29], '\004')[0]/1000 as max_fr,
    if(get_json_object(ext_dsp,'$.price_out')/1000/100>split(split(ext_algo, ',')[29], '\004')[0]/1000,'1','0') as price_cmp,
	round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1) as disc_cpm,
	round(log10(get_json_object(ext_dsp,'$.price_out')/100), 1) as disc_bidprice
	from dwh.ods_adn_trackingnew_impression where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and ad_type in('rewarded_video','interstitial_video','sdk_banner') 
	and get_json_object(ext_dsp,'$.is_hb')=1
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	ins_sql="select campaign_id, requestid, created,
	split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner')
	"

	sql="select dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id,calibration, ad_type, price_cmp,disc_cpm,disc_bidprice,
	count(requestid) as imp, sum(cost) as cost, sum(max_fr) as max_fr, sum(rev) as rev 
    from (select a.requestid as requestid, a.dtm as dtm, a.app_id as app_id, a.unit_id as unit_id, a.price_cmp as price_cmp, 
	a.disc_bidprice as disc_bidprice, a.disc_cpm as disc_cpm,
    a.ad_type as ad_type, a.calibration as calibration, a.country_code as country_code, a.dsp_id as dsp_id, a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
	a.cost as cost, b.rev as rev
	from($imp_sql)a left join ($ins_sql)b on a.requestid=b.requestid and b.created-a.created<=$ins_ret_time*3600)c
	group by dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id, calibration, ad_type, price_cmp,
	disc_cpm,disc_bidprice;"

	echo "sql:$sql"
	hive -e "$sql" >$output
}

function query_winrate_bidprice_discrete() {
	begin_date=$1
	end_date=$2

	if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
		echo "ERROR:begin_date or end_date is invalid."
		exit
	fi

	unit_id_condition='is not null'
    unit_id_condition="in(339986, 329384, 206663,198608,272281,269332,206661,249273,293668,280124,292164,280459)"
	country_code_condition="in('us','cn')"

	#get win/loss of each token
	hb_event="select concat(yyyy,mm,dd) as dtm, publisher_id, app_id, unit_id, dsp_id,
	split(extra3, '-')[3] as strategy_name,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, city_code, ad_type, platform,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(token, '_')[0] as token, 
	(price/100/1000) as bidprice,
	split(ext_adx_algo,'\004')[2] as optimized_bidprice,
	split(ext_adx_algo,'\004')[3] as predicted_bidprice,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
	round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1) as disc_cpm,
    round(log10(price/100), 1) as disc_bidprice,
	(case event_type when '1' then 'win' else 'loss' end) as event_type
	from dwh.ods_adn_hb_v1_event 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video')
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	#get price_out of each token in order to figure cost
	adx_imp="select concat(yyyy,mm,dd) as dtm, request_id, ch_request_id,
	(price/100/1000) as price_out, (dsp_price_in/100/1000) as price_raw  
	from dwh.ods_adn_adx_tracking_v1_impression 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and is_hb='1'
	and ch_spot_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	sql="select event.dtm, event.event_type, event.ad_type, event.unit_id, event.country_code, event.platform, event.dsp_id, event.strategy_name,
	event.calibration,event.disc_bidprice,event.disc_cpm,
	sum(event.bidprice) as sum_bidprice,
	sum(event.optimized_bidprice) as sum_optimized_bidprice,
	sum(event.predicted_bidprice) as sum_predicted_bidprice,
	sum(event.max_fillrate_score) as sum_max_fillrate_score,
	count(DISTINCT event.token) as req, count(DISTINCT imp.request_id) as imp, 
	sum(imp.price_out) as cost, sum(imp.price_raw) as price_raw
	from ($hb_event) event 
	left join ($adx_imp) imp on event.token=imp.request_id
	group by event.dtm, event.calibration, event.event_type, event.ad_type, event.unit_id, event.country_code, 
	event.platform, event.dsp_id, event.strategy_name,
    event.disc_bidprice,event.disc_cpm
	;"

	echo "$sql"
	hive -e "$sql" >output/hb_winrate_price_discrete_${begin_date}_${end_date}.txt
}

function query_roi_bidprice_discrete() {
	beg_date=${1}
	end_date=${2}

	output=output/${beg_date}_${end_date}_hb_roi_bidprice_discrete.txt
	unit_id_condition='in(206663,198608,272281,269332,206661,249273,293668,280124,292164,280459)'
	#unit_id_condition='is not null'
	country_code_condition="in('us', 'cn')"
	ins_ret_time=48

	imp_sql="select campaign_id, requestid, created, concat(yyyy,mm,dd) as dtm, app_id, unit_id, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, split(split(strategy, '\\;')[7], '-')[3] as strategy_name, 
	platform, 
	get_json_object(ext_dsp,'$.price_out')/1000/100 as cost, get_json_object(ext_dsp,'$.dspid') as dsp_id,
	split(split(ext_algo, ',')[28], '\004')[7] as calibration,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fr,
	round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1) as disc_cpm,
	round(log10(get_json_object(ext_dsp,'$.price_out')/100), 1) as disc_bidprice
	from dwh.ods_adn_trackingnew_impression
	where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
	and ad_type in('rewarded_video','interstitial_video','sdk_banner') 
	and get_json_object(ext_dsp,'$.is_hb')=1
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

	ins_sql="select campaign_id, requestid, created,
	split(ext_algo, ',')[2] as rev 
    from dwh.ods_adn_trackingnew_install
    where concat(yyyy,mm,dd,hh)>=${beg_date} and concat(yyyy,mm,dd,hh)<=${end_date} 
    and ad_type in('rewarded_video','interstitial_video','sdk_banner')
	"

	sql="select dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id, calibration, ad_type,
	count(requestid) as imp, sum(cost) as cost, sum(max_fr) as max_fr, sum(rev) as rev,
	disc_cpm, disc_bidprice
    from 
    (select a.requestid as requestid, a.dtm as dtm, a.app_id as app_id, a.unit_id as unit_id, 
    a.ad_type as ad_type, a.calibration as calibration, a.country_code as country_code, a.dsp_id as dsp_id, 
    a.strategy_name as strategy_name, a.platform as platform, a.max_fr as max_fr,
    a.disc_bidprice as disc_bidprice, a.disc_cpm as disc_cpm,
	a.cost as cost, b.rev as rev
	from($imp_sql)a left join ($ins_sql)b 
	on a.requestid=b.requestid)c
	group by dtm, app_id, unit_id, country_code, strategy_name, platform, dsp_id, calibration, ad_type, disc_cpm, disc_bidprice;"

	echo "sql:$sql"
	hive -e "$sql" >$output
}


function query_HB_req_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_ids="483792,209912,433756,439274,343784,512355,474230,448418"
    app_ids="145734"
    app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    unit_id_condition="is not null"
    #unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
    app_id,
	split(split(strategy, '\\;')[7], '-')[3] as strategy_name,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
	if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, app_id, 
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
	split(split(strategy, '\\;')[7], '-')[3],
	if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/hb_req_by_time_${beg_date}_${end_date}.txt
}


function query_HB_req_hourly() {
    beg_date=${1}
    end_date=${2}

    unit_ids="505609"
    #app_ids="145734"
    #app_id_condition="in($app_ids)"
    app_id_condition="is not null"
    #unit_id_condition="is not null"
    unit_id_condition="in($unit_ids)"
    country_code_condition="in('cn', 'us')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp, 
	unit_id,
    split(split(ext_algo, ',')[20], '\004')[0] tpl_trynew,
	split(ext_algo, ',')[0] as algo_cam,
    split(split(ext_algo, ',')[20], '\004')[1] video_trynew,
    split(split(ext_algo, ',')[20], '\004')[2] playable_trynew,
    if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other') as country_code, 
	if(extra2!='', 'fill', 'no_fill') as if_fill, 
	split(split(ext_algo, ',')[45], '\004')[0] as region,
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm, 
    sum(split(split(ext_algo, ',')[26], '\004')[3]) as sum_cr_ivr, 
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score, 
    sum(split(split(ext_algo, ',')[24], '\004')[0]) as sum_imp_control_rate, 
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_hb_request 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), ad_type, 
	unit_id,
	split(split(ext_algo, ',')[45], '\004')[0],
	split(ext_algo, ',')[0],
	if(lower(country_code) in('cn','us', 'jp', 'uk'),lower(country_code),'other'), 
    split(split(ext_algo, ',')[20], '\004')[0],
    split(split(ext_algo, ',')[20], '\004')[1],
    split(split(ext_algo, ',')[20], '\004')[2],
    if(extra2!='', 'fill', 'no_fill'), split(ext_algo, ',')[40]
    ;"

    echo "$sql"
    hive -e "$sql" >output/hb_req_hourly_${beg_date}_${end_date}.txt
}

#sh hb_factor_get.sh

#query_HB_req_by_time 2021082000 2021082423

query_HB_req_hourly 2021112000 2021112523 &
query_HB_req_hourly 2021112000 2021112523 &

#query_winrate_bidprice_discrete 2020082500 2020093123 &
#query_roi_bidprice_discrete 2020082800 2020083123 &
#query_hb_price_compare_from_tracking 2020073000 2020080300 &
#query_hb_moreoffer_effect_from_tracking 2020072000 2020072623 &
#query_hb_houly_cpm_detail 2020070900 2020071323 &
#query_hb_houly_cpm_detail_with_campaign 2020083000 2020093023 &
#query_hb_effect_from_tracking 2020091000 2020093023 &
#query_hb_effect_from_tracking_with_campaign 2020090500 2020093123 &
#query_bidrate_cost 2020080100 2020083123 &
#query_bidrate_roi 2021081000 2020081023
#query_hb_cpm_predict 2020070700 2020070723 &
#query_hb_bidprice_and_cpm_detail 2020051100 2020051123
#query_detailed_info_for_hb 2020081800 2020083023 &
#query_hb_effect 2020072000 2020072623 &
