

function query_portal_hourly() {
    beg_date=$1
    end_date=$2
    beg_date1=$(date -d "-7 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date1=$(date -d "-7 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")
    beg_date2=$(date -d "-1 day ${beg_date:0:8} ${beg_date:8:2}" +"%Y%m%d%H")
    end_date2=$(date -d "-1 day ${end_date:0:8} ${end_date:8:2}" +"%Y%m%d%H")

    if [ ${#beg_date} -ne 10 ] || [ ${#end_date} -ne 10 ]; then
        echo "ERROR:beg_date or end_date is invalid."
        exit
    fi
    #if(lower(country_code) in('cn','us','jp','uk'), lower(country_code),'other') as country_code, 

    unit_id_condition="is not null"
    #country_code_condition="in('us', 'cn')"
    country_code_condition="is not null"

    sql="
        select
            pdate,
            phour,
            ad_type,
			sum(normal_request_times) as request_n,
            sum(original_impression) as impression_n,
            sum(original_click) as click_n,
            sum(original_install) as install_n,
            sum(original_money_v2 / 1000.0) as revenue,
            sum(money_v2 / 1000.0) as cost
        from
            adn_seesaw.report_v2_hour
        where
            ((concat(pdate,phour)>=$beg_date and concat(pdate,phour)<=$end_date) 
            or (concat(pdate,phour)>=$beg_date1 and concat(pdate,phour)<=$end_date1)
            or (concat(pdate,phour)>=$beg_date2 and concat(pdate,phour)<=$end_date2))
            and scenario in (0, 'openapi')
            and match_v2 in (0, 3)
            and ad_type in('rewarded_video','interstitial_video', 'sdk_banner', 'more_offer', 'native')
            and unit_id $unit_id_condition
            and country_code $country_code_condition
        group by
			ad_type,
            pdate,
            phour;
        "
    echo "$sql"
    hive -e "$sql" >output/protal_data_${begin_date}_${end_date}.txt
}

query_portal_hourly 2021102500 2021102823
