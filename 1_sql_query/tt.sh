#########################################################################
# File Name: tt.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 01 Mar 2022 11:05:48 AM CST
#########################################################################
#!/bin/bash

#and ad_type in('rewarded_video','interstitial_video')


sql="select 
    ad_type,
    if(strategy like '%;No;%', 'm_base', (if(strategy like '%;Yes;%', 'm_exp', 'other'))) as group_name,
    count(*) as req
from dwh.ods_adn_trackingnew_hb_request
where 
    strategy like 'MNormalAlpha%' 
    and concat(yyyy,mm,dd) between '20220301' and '20220301'
group by
    ad_type,
    if(strategy like '%;No;%', 'm_base', (if(strategy like '%;Yes;%', 'm_exp', 'other')))
;"

hive -e "$sql" > tt.out
