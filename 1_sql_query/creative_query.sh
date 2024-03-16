unit_ids='390260,389620,104819,378329'

function query_creative_effect(){
	beg_date=$1
	end_date=$2

	if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_ids='429354'
	unit_id_condition="in($unit_ids)"
	#unit_id_condition="is not null"
    #country_code_condition="in('us','cn')"
	country_code_condition="in('id')"
	#publisher_ids='10714'
	#publisher_id_condition="in($publisher_ids)"
	publisher_id_condition="is not null"

	sql="select 
	concat(year,month,day) as dtm,
	campaign_id,
	bucket_id,
	ad_type, app_id, country_code, big_template_id,
	unit_id,
	templategroup, videotemplate,
	image, video, playable,
	sum(impression) as imp,
	sum(click) as clk, sum(install) as ins,
	sum(revenue) as rev
	from  monitor_effect.creative_info_table 
	where concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video', 'sdk_banner') 
	and unit_id $unit_id_condition
	and (bucket_id like '1%')
    and lower(country_code) $country_code_condition
	and publisher_id $publisher_id_condition
	group by 
	bucket_id,
	campaign_id,
	templategroup, videotemplate,
	concat(year,month,day),
	ad_type, app_id, country_code,
	big_template_id,
	unit_id,
	image, video, playable
	;"

	echo $sql
	hive -e "$sql" > output/creative_effect_${beg_date}_${end_date}.txt

}

function query_big_template_effect(){
	beg_date=$1
	end_date=$2

	if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi

    unit_ids='451906,443713'
	unit_id_condition="in($unit_ids)"
	unit_id_condition="is not null"
    country_code_condition="is not null"
	#country_code_condition="in('cn', 'us')"
	#publisher_ids='10714'
	#publisher_id_condition="in($publisher_ids)"
	publisher_id_condition="is not null"

	sql="select 
	concat(year,month,day) as dtm, app_id,
	bucket_id,
	ad_type,
	platform,
	if(lower(country_code) in('cn','us'), lower(country_code),'other') as country_code,
	big_template_id,
	sum(impression) as imp,
	sum(click) as clk, sum(install) as ins,
	sum(revenue) as rev
	from  monitor_effect.creative_info_table 
	where concat(year,month,day,hour)>=$beg_date and concat(year,month,day,hour)<=$end_date 
	and ad_type in('rewarded_video','interstitial_video', 'sdk_banner') 
	and unit_id $unit_id_condition
	and (bucket_id like '1%')
    and lower(country_code) $country_code_condition
	and publisher_id $publisher_id_condition
	and campaign_id not in('223275881')
	group by 
	app_id,
	bucket_id,
	concat(year,month,day),
	ad_type, 
	if(lower(country_code) in('cn','us'), lower(country_code),'other'),
	platform,
	big_template_id
	;"

	echo $sql
	hive -e "$sql" > output/big_template_effect_${beg_date}_${end_date}.txt

}

#query_big_template_effect 2021062000 2021062423
query_creative_effect 2021101900 2021101923
