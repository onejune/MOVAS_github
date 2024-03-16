#########################################################################
# File Name: temp.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 10 Mar 2022 02:12:10 PM CST
#########################################################################
#!/bin/bash

hive -e" select concat(yyyy,mm,dd) as dtm, ext_endcard,ext_rv_template,ad_type,platform,ext_bigTemplateId,
split(ext_algo, ',')[15] as algo_video_tpl,
split(ext_algo, ',')[16] as algo_endcard_tpl,
count(*) as imp
from dwh.ods_adn_trackingnew_impression
where 
concat(yyyy,mm,dd) >= '20220201' and  concat(yyyy,mm,dd) <= '20220305'
and strategy like '%MNormalAlpha%'
and ad_type in('rewarded_video','interstitial_video')
and lower(country_code) in('us')
group by 
concat(yyyy,mm,dd), 
ext_endcard,ext_rv_template,ad_type,platform,ext_bigTemplateId,
split(ext_algo, ',')[15],
split(ext_algo, ',')[16]
;" > template.txt
