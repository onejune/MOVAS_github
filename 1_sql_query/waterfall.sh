function query_fillrate_by_time() {
    beg_date=${1}
    end_date=${2}

    unit_ids='277900,277902,277904,277906,277908,277910'
    app_ids='131991'
    app_ids='127738'
	app_id_condition="in($app_ids)"
	#unit_id_condition="in($unit_ids)"
    unit_id_condition="is not null"
    country_code_condition="in('us','cn')"

    sql="select 
    concat(yyyy,mm,dd) as dtm, ad_type,
    if(lower(country_code) in('cn','us'),lower(country_code),'other') as country_code, 
    unit_id, if(extra2!='', 'fill', 'no_fill') as if_fill, 
	round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1) as disc_cpm,
	sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
	count(*) as req
	from dwh.ods_adn_trackingnew_request 
    where 
    (concat(yyyy,mm,dd,hh)>=$beg_date and concat(yyyy,mm,dd,hh)<=$end_date) 
    and unit_id $unit_id_condition
	and app_id $app_id_condition
	and strategy like 'MNormal%'
	and ad_type in('rewarded_video','interstitial_video')
	group by concat(yyyy,mm,dd), ad_type, unit_id, if(extra2!='', 'fill', 'no_fill'), round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1), 
	if(lower(country_code) in('cn','us'),lower(country_code),'other')
    ;"

    echo $sql
    hive -e "$sql" >output/waterfall_127738_${beg_date}_${end_date}.txt
}

function query_bidrate_cost() {
    begin_date=$1
    end_date=$2

    if [ ${#begin_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_id_condition="in('297098','297274')"
    unit_id_condition="in('244112','244110')"
    country_code_condition="in('us','cn')"

    #get win/loss of each token
    hb_event="select concat(yyyy,mm,dd) as dtm, ad_type, 
	if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other') as country_code, unit_id,
	(case event_type when '1' then 'win' else 'loss' end) as event_type,
	split(extra3, '-')[3] as strategy_name,
    round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1) as disc_cpm,
	round(log10(price/100), 1) as disc_bidprice,
	sum(split(split(ext_algo, ',')[29], '\004')[0]) as sum_max_fillrate_score,
	sum(price/100) as sum_bidprice,
	count(*) as req
    from dwh.ods_adn_hb_v1_event 
    where concat(yyyy,mm,dd,hh)>=$begin_date and concat(yyyy,mm,dd,hh)<=$end_date 
    and ad_type in('rewarded_video','interstitial_video')
    and unit_id $unit_id_condition
    and lower(country_code) $country_code_condition
    group by concat(yyyy,mm,dd), unit_id,
    if(lower(country_code) in('cn','us','jp','uk'),lower(country_code),'other'),
    ad_type,
	split(extra3, '-')[3],
    round(log10(split(split(ext_algo, ',')[29], '\004')[0]), 1),
	round(log10(price/100), 1),
    (case event_type when '1' then 'win' else 'loss' end)"

    echo "$hb_event"
    hive -e "$hb_event" > output/hb_cpm_discrete_${begin_date}_${end_date}.txt
}

query_bidrate_cost 2020061000 2020061523 &
#query_fillrate_by_time 2020061300 2020061723 &
