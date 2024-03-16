#########################################################################
# File Name: t.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 21 Dec 2021 05:09:47 PM CST
#########################################################################
#!/bin/bash

set hive.groupby.orderby.position.alias=true;
set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint=false;

sql="
select
    req_table.day,
    req_table.requestsource,
    case 
        when req_table.experiment='open' then '5%实验组'
        when req_table.experiment='close' then '5%对照组'
        when req_table.experiment='default' then '未标记'
        else '非网赚流量'
    end as experiment,
    req_table.cam_type,
    req_table.strategy,
    
    count(1) as req_cnt, 
    round( sum(req_table.bidprice) / count(1), 2) as req_bid_price,
    
    sum(if(imp_table.requestid is null, 0, 1)) as imp_cnt, 
    
    sum(if(clk_table.requestid is null, 0, 1)) as clk_cnt, 
    
    sum(if(ins_table.requestid is null, 0, 1)) as ins_cnt, 
    
    round( sum(if(imp_table.requestid is null, 0, imp_table.hb_price)), 2 ) as hb_cost,
    round( sum(if(imp_table.requestid is null, 0, if (unitcc.cc_cost>0, unitcc.cc_cost, unitall.all_cost)))/1000, 2 ) as wf_cost,
    round( sum(if(ins_table.requestid is null, 0, ins_table.single_rev)), 2 ) as rev
from
(
select
    concat(yr,mt,dt) as day,
    get_json_object(usedadkeys, '$.make_money_dsp_switch') as experiment,
    case
	   when strategy like '%MNormalAlphaModelRankerHH%' then 'as'
	   when strategy like '%MNormalAlphaModelRankerSS%' then 'mmdsp'
	end as strategy,
	
    case 
        when split(returncampaignlist, ',')[0] in ('374072652', '374158585', '374186238', '374358933', '374519991', '374562622', '374581624', '374647320', '374715929', '374726868', '374780371', '374780723', '374827851', '374932626', '374968532', '375001775', '375019122', '375019407', '375022962', '375120757', '375181186', '375256115', '375256939', '375263309', '375317617') then 'make-money' 
        else 'light' 
	end as cam_type, 
	requestsource,
    bidprice,
    
    requestid
from
    adn_dsp.log_pioneer_access
where
    concat(yr,mt,dt) = '20211219'
    and rg='singapore'
    and code='200'
    and get_json_object(usedadkeys, '$.make_money_dsp_switch') in ('open', 'close')
    and (strategy like '%MNormalAlphaModelRankerHH%' or strategy like '%MNormalAlphaModelRankerSS%')
) req_table
left join 
( 
	select 
	    case
	        when strategy like '%MNormalAlphaModelRankerHH%' then 'as'
	        when strategy like '%MNormalAlphaModelRankerSS%' then 'mmdsp'
	    end as strategy,
        ext_bidsPrice / 1000 as imp_bid_price, 
        if(get_json_object(ext_dsp, '$.is_hb') = '1', get_json_object(ext_dsp, '$.price_out')/1000/100, 0) as hb_price,
        
        unit_id,
        country_code,
        requestid

	from 
	    dwh.ods_adn_trackingnew_impression 

	where 
	    concat(yyyy, mm, dd) = '20211219'
	    and get_json_object(ext_asabtestrestag, '$.make_money_dsp_switch') is not null 
	    and publisher_id<>'6028'


) imp_table on req_table.requestid = imp_table.requestid and req_table.strategy = imp_table.strategy
left join 
( 
	select 
	    case
	        when strategy like '%MNormalAlphaModelRankerHH%' then 'as'
	        when strategy like '%MNormalAlphaModelRankerSS%' then 'mmdsp'
	    end as strategy,
        ext_bidsPrice / 1000 as clk_bid_price, 
        
        requestid

	from 
	    dwh.ods_adn_trackingnew_click

	where 
	    concat(yyyy, mm, dd) = '20211219'
	    and get_json_object(ext_asabtestrestag, '$.make_money_dsp_switch') is not null 
	    and publisher_id<>'6028'


) clk_table on imp_table.requestid = clk_table.requestid and imp_table.strategy = clk_table.strategy 
left join 
( 
	select 
	    case
	        when strategy like '%MNormalAlphaModelRankerHH%' then 'as'
	        when strategy like '%MNormalAlphaModelRankerSS%' then 'mmdsp'
	    end as strategy,
        ext_bidsPrice / 1000 as ins_bid_price, 
        if(size(split(ext_bp, '"')) > 1, split(ext_bp, '"')[1], split(substr(ext_bp, 2), ',')[0]) as single_rev,
        
        requestid

	from 
	    dwh.ods_adn_trackingnew_install

	where 
	    concat(yyyy, mm, dd) = '20211219'
	    and get_json_object(ext_asabtestrestag, '$.make_money_dsp_switch') is not null 
	    and publisher_id<>'6028'


) ins_table on clk_table.requestid = ins_table.requestid and  clk_table.strategy = ins_table.strategy
left JOIN ( select unitid, countrycode, cost as cc_cost from mmdsp.unit_cc_cost where countrycode<>'ALL' ) as unitcc on imp_table.unit_id=unitcc.unitid and unitcc.countrycode=imp_table.country_code
left JOIN ( select unitid, cost as all_cost from mmdsp.unit_cc_cost where countrycode='ALL' ) as unitall on imp_table.unit_id=unitall.unitid

group by 
    1,2,3,4,5
";

hive -e "$sql" > res.txt
