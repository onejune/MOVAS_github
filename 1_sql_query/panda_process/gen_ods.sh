function query_creative_effect() {
    beg_date=$1
    end_date=$2

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:begin_date or end_date is invalid."
        exit
    fi
    unit_id_condition="is not null"
	#country_code_condition="in('cn', 'us', 'jp', 'uk', 'ca', 'au', 'de', 'br')"
	country_code_condition="is not null"
    publisher_id_condition="is not null"

    sql="select 
	ad_type, app_id, unit_id, country_code,
	templategroup, videotemplate,endcardtemplate,
	packagename,campaign_id,video,playable,image,
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
	ad_type, app_id, unit_id, country_code,
	templategroup, videotemplate,endcardtemplate,
	packagename,campaign_id,video,playable,image
	;"

    echo $sql
    hive -e "$sql" >output/creative_effect.dat
}

beg_date=$(date +%Y%m%d%H -d "1 days ago")
end_date=$(date +%Y%m%d%H -d "0 hours ago")
echo "query creative effect from $beg_date to $end_date......"

start_second=$(date +%s)
query_creative_effect $beg_date $end_date
end_second=$(date +%s)
echo "query data used time: "$((end_second-start_second))"s"

echo "build ods......"
output=temp/cerr.dat
echo "" > $output

start_second=$(date +%s)
python ods_pd_process.py output/creative_effect.dat $output
#python creative_ods_pd.py output/tem.dat $output
end_second=$(date +%s)
echo "build ods used time: "$((end_second-start_second))"s"

grep -v "key" $output | awk '{if(NR>3){print $1,"\t",$7,"\t",$2,"\t",$3}}' > creative3_creative_cpm_new.dat
