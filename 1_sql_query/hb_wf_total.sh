function query_hb_bidrate_cost_total() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition='is not null'
    country_code_condition="is not null"

    #get win/loss of each token
    hb_event="select concat(yyyy,mm,dd) as dtm, 
    bidid as requestid, 
    ad_type, dsp_id,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, platform,
	(price/100/1000) as bidprice,
	split(split(ext_algo, ',')[29], '\004')[0] as max_fillrate_score,
	(case event_type when '1' then 'win' else 'loss' end) as event_type
	from dwh.ods_adn_hb_v1_bid 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
    and extra3 like 'MNormalAlpha%'
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

    #get price_out of each token in order to figure cost
<<<<<<< HEAD
    adx_imp="select requestid, 
	get_json_object(ext_dsp,'$.price_out')/1000/100 as price_out
=======
    adx_imp="select concat(yyyy,mm,dd) as dtm, 
    requestid, 
    get_json_object(ext_dsp,'$.dspid') as dsp_id,
	sum(get_json_object(ext_dsp,'$.price_out'))/1000/100 as price_out
>>>>>>> 53745585e93afd713e15843a69c921379481eeae
	from dwh.ods_adn_trackingnew_impression 
	where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
	and unit_id $unit_id_condition
	and lower(country_code) $country_code_condition"

    sql="select event.dtm, event.event_type, 
    event.ad_type, event.country_code, event.platform, event.dsp_id,
	sum(event.bidprice) as sum_bidprice,
	sum(event.max_fillrate_score) as sum_max_fillrate_score,
	count(DISTINCT event.requestid) as req, 
    count(DISTINCT imp.requestid) as imp, 
	sum(imp.price_out) as price_out
	from ($hb_event) event 
	left join ($adx_imp) imp on event.requestid=imp.requestid
	group by event.dtm, event.event_type, event.ad_type, event.country_code, 
	event.platform, event.dsp_id
	;"

    echo "$sql"
    hive -e "$sql" >output/hb_bidrate_${begin_date}_${end_date}.txt
}

function query_wf_fillrate() {
    beg_date=${1}
    end_date=${2}

    unit_id_condition='is not null'
    country_code_condition="is not null"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type, split(ext_algo, ',')[40] as if_lower_imp,
    if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, 
    platform, 
	if(extra2!='', 'fill', 'no_fill') as if_fill, 
    sum(split(split(ext_algo, ',')[26], '\004')[1]) as sum_cr_cpm,
	sum(split(ext_algo, ',')[3]) as sum_cr_ivr,
    sum(split(split(ext_algo, ',')[25], '\004')[1]) as sum_fr_cpm_pre, 
    sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
    sum(split(ext_algo, ',')[39]) as sum_bid_floor, 
    sum(split(ext_algo, ',')[38]) as sum_ecpm_floor, 
    sum(split(ext_algo, ',')[2]) as sum_price,
    count(*) as req 
    from dwh.ods_adn_trackingnew_request
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    and strategy like 'MNormalAlpha%'
    group by concat(yyyy,mm,dd), ad_type, 
	platform,
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'), 
    if(extra2!='', 'fill', 'no_fill'), 
	split(ext_algo, ',')[40]
	;"

    echo $sql
    hive -e "$sql" >output/wf_fillrate_${beg_date}_${end_date}.txt
}

function query_daily_roi() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
    #if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code,

    unit_id_condition='is not null'
    country_code_condition="is not null"

    sql="select substr(req_time,1,8) as dtm, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code, 
	is_hb, 
	platform,
    sum(total_req) as req, 
	sum(adx_impression) as adx_imp,
	sum(impression) as nor_imp,
	sum(impression)+sum(adx_impression) as total_imp,
	sum(click) as click,
	sum(normal_install) as install,
	sum(adx_revenue) as adx_rev,
	sum(normal_revenue) as nor_rev,
    sum(adx_revenue)+sum(normal_revenue) as total_rev,
	sum(cost) as nor_cost,
	sum(adx_cost) as adx_cost,
    sum(cost)+sum(adx_cost) as total_cost
    from monitor_effect.model_effect_info_test 
    where concat(year,month,day,hour)>='$begin_date' and concat(year,month,day,hour)<='$end_date'
    and req_time>='$begin_date' and req_time<='$end_date'
	and (bucket like '1%')
	and unit_id $unit_id_condition
	and campaign_id!='371092647'
	and country_code $country_code_condition
	group by substr(req_time,1,8), 
    ad_type, 
	platform,
	if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other'),
	is_hb
	;"

    echo "$sql"
    hive -e "$sql" >output/days_effect_${begin_date}_${end_date}.txt
}

query_hb_bidrate_cost_total 2021082200 2021082223 &
<<<<<<< HEAD
#query_wf_fillrate 2021082200 2021082223 &
#query_daily_roi 2021082200 2021082223 &

=======
query_wf_fillrate 2021082200 2021082223 &
query_daily_roi 2021082200 2021082223 &
>>>>>>> 53745585e93afd713e15843a69c921379481eeae
