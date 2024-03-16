#########################################################################
# File Name: query_third_dsp.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 28 Apr 2022 10:48:53 PM CST
#########################################################################
#!/bin/bash

sql="
select pv_t.dspid as dspid, pv_t.bundleid as bundleid, pv_t.ad_type as ad_type,
    count(1) as pv, 
    sum(case when imp_t.requestid is not null then 1 end) as impression, 
    sum(case when cli_t.requestid is not null then 1 end) as click,
    sum(pv_t.ssp_cost) as cost, sum(pv_t.dsp_cost) as revenue
from (
    SELECT requestid, expect_cost as ssp_cost, 
    get_json_object(ext_dsp, '$.dspid') as dspid, 
    get_json_object(ext_dsp, '$.price_in') as dsp_cost, 
    get_json_object(ext_dsp, '$.bd') as bundleid,
    ad_type
    FROM dwh.ods_adn_trackingnew_ssp_pv
    WHERE concat(yyyy, mm, dd) = '20220427' and hh=12
        AND scenario = 'openapi'
        AND request_type in ('7','10')
        AND get_json_object(ext_dsp, '$.dspid') is null and get_json_object(ext_dsp, '$.dspid') not in (0,13,42,2,35,64)
) as pv_t
left join (
    select requestid
    from dwh.ods_adn_trackingnew_ssp_impression
    WHERE concat(yyyy, mm, dd) = '20220427' and hh=12
        AND scenario = 'openapi'
        AND request_type in ('7','10')
        AND get_json_object(ext_dsp, '$.dspid') is null and get_json_object(ext_dsp, '$.dspid') not in (0,13,42,2,35,64)
    group by requestid
) as imp_t on (pv_t.requestid=imp_t.requestid)
left join (
    select requestid
    from dwh.ods_adn_trackingnew_ssp_click
    WHERE concat(yyyy, mm, dd) = '20220427' and hh=12
        AND scenario = 'openapi'
        AND request_type in ('7','10')
        AND get_json_object(ext_dsp, '$.dspid') is null and get_json_object(ext_dsp, '$.dspid') not in (0,13,42,2,35,64)
    group by requestid
) as cli_t on (pv_t.requestid=cli_t.requestid)
group by pv_t.dspid, pv_t.bundleid, pv_t.ad_type
;"

