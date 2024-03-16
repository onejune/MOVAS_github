import sys
import math
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import split, explode, concat, concat_ws
import datetime
import time
import pytz

start_hour="2022060614"
tz = pytz.timezone('Asia/Shanghai')  # 东八区
time_now = datetime.datetime.fromtimestamp(int(time.time()), tz)
now = time_now.strftime('%Y%m%d%H')
end_hour = (time_now - datetime.timedelta(hours=3)).strftime("%Y%m%d%H")
install_end_hour = now

sql = """
   SELECT exp_id, devid_type, plf, cc,bidding_type, client_type, cam_type, unit_id, campaign_id, recall_rank
    , count(1) AS imp, sum(p_ivr) AS p_ins
    , sum(if(req IS NULL, 0, 1)) AS ins
    , sum(cost) AS cost, sum(rev) AS rev
    FROM (
    SELECT req, exp_id, cc, plf, bidding_type, cam_type, campaign_id, recall_rank, unit_id
        , client_type, p_ivr
        , cost_cpc + cost_cpi + cost_cpm + cost_hb AS cost
        , rev_cpc + rev_cpi + rev_cpm AS rev
        , CASE 
            WHEN plf = 'ios'
                AND idfa_cnt = 1
            THEN 'idfa'
            WHEN plf = 'ios'
                AND idfa_cnt = 0
                AND sysid_cnt = 1
            THEN 'sysid'
            WHEN plf = 'ios'
                AND idfa_cnt = 0
                AND sysid_cnt = 0
                AND idfv_cnt = 1
            THEN 'idfv'
            WHEN plf = 'ios'
                AND idfa_cnt = 0
                AND sysid_cnt = 0
                AND idfv_cnt = 0
                AND bkupid_cnt = 1
            THEN 'bkupid'
            WHEN plf = 'ios'
                AND idfa_cnt = 0
                AND sysid_cnt = 0
                AND idfv_cnt = 0
                AND bkupid_cnt = 0
            THEN 'ios-unkonwn'
            WHEN plf = 'android'
                AND gaid_cnt = 1
            THEN 'gaid'
            WHEN plf = 'android'
                AND gaid_cnt = 0
            THEN 'android-no-gaid'
            ELSE 'unknown'
        END AS devid_type
    FROM (
        SELECT D.requestid AS req, exp_id, cc, plf, bidding_type, cam_type, campaign_id, recall_rank, unit_id
            , client_type, p_ivr, rev_cpm, cost_cpm
            , if(length(gaid) = 36
                AND gaid != '00000000-0000-0000-0000-000000000000', 1, 0) AS gaid_cnt
            , if(length(idfa) = 36
                AND idfa != '00000000-0000-0000-0000-000000000000', 1, 0) AS idfa_cnt
            , if(length(sysid) = 36
                AND sysid != '00000000-0000-0000-0000-000000000000', 1, 0) AS sysid_cnt
            , if(length(idfv) = 36
                AND idfv != '00000000-0000-0000-0000-000000000000', 1, 0) AS idfv_cnt
            , if(length(bkupid) = 36
                AND bkupid != '00000000-0000-0000-0000-000000000000', 1, 0) AS bkupid_cnt
            , if(B.requestid IS NULL, 0, cost_hb) AS cost_hb
            , if(C.requestid IS NULL, 0, cost_cpc) AS cost_cpc
            , if(D.requestid IS NULL, 0, cost_cpi) AS cost_cpi
            , if(C.requestid IS NULL, 0, rev_cpc) AS rev_cpc
            , if(D.requestid IS NULL, 0, rev_cpi) AS rev_cpi
        FROM (
            SELECT requestid, campaign_id, Platform AS plf, split(ext_algo, ',')[3] AS p_ivr, split(strategy, '\\;')[5] as cam_type,
                split(split(ext_algo, ',')[30], '\004')[0] as recall_rank, unit_id
                , CASE 
                    WHEN get_json_object(ext_data2, '$.expIds') LIKE '%1231%' THEN '1231'
                    WHEN get_json_object(ext_data2, '$.expIds') LIKE '%1232%' THEN '1232'
                    WHEN get_json_object(ext_data2, '$.expIds') LIKE '%1233%' THEN '1233'
                    ELSE 'unknown'
                END AS exp_id
                , CASE 
                    WHEN lower(country_code) IN ('us') THEN 'us'
                    ELSE 'no-us'
                END AS cc
                , CASE 
                    WHEN get_json_object(ext_dsp, '$.is_hb') = 1 THEN 'hb'
                    ELSE 'wf'
                END AS bidding_type
                , CASE 
                    WHEN strategy LIKE '%MNormalAlphaModelRankerHH%' THEN 'nrs'
                    WHEN strategy LIKE '%MNormalAlphaModelRankerSS%' THEN 'srs'
                    ELSE 'unknown'
                END AS client_type, gaid, idfa, split(ext_sysId, ',')[0] AS sysid, split(cdn_ab, ',')[0] AS idfv
                , split(ext_sysId, ',')[1] AS bkupid
                , if(size(split(ext_bp, '\\"')) > 1, split(ext_bp, '\\"')[1], split(substr(ext_bp, 2), ',')[0])/1000 AS rev_cpm
                , expect_cost / 1000 AS cost_cpm
            FROM dwh.ods_adn_trackingnew_ssp_impression
            WHERE concat(yyyy, mm, dd, hh) >= '{0}'
                AND concat(yyyy, mm, dd, hh) <= '{1}'
                  AND (get_json_object(ext_data2, '$.expIds') LIKE '%1231%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1232%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1233%')
                AND ad_type IN ('interstitial_video', 'rewarded_video')
        ) A
            LEFT JOIN (
                SELECT requestid, expect_cost / 1000 AS cost_hb
                FROM dwh.ods_adn_trackingnew_ssp_pv
                WHERE concat(yyyy, mm, dd, hh) >= '{0}'
                    AND concat(yyyy, mm, dd, hh) <= '{1}'
                      AND (get_json_object(ext_data2, '$.expIds') LIKE '%1231%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1232%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1233%')
                    AND ad_type IN ('interstitial_video', 'rewarded_video')
                    AND get_json_object(ssp_pv_info, '$.rej_t') IS NULL
            ) B
            ON A.requestid = B.requestid
            LEFT JOIN (
                SELECT requestid, campaign_id, expect_cost AS cost_cpc
                    , if(size(split(ext_bp, '\\"')) > 1, split(ext_bp, '\\"')[1], split(substr(ext_bp, 2), ',')[0]) AS rev_cpc
                FROM dwh.ods_adn_trackingnew_ssp_click
                WHERE concat(yyyy, mm, dd, hh) >= '{0}'
                    AND concat(yyyy, mm, dd, hh) <= '{1}'
                     AND (get_json_object(ext_data2, '$.expIds') LIKE '%1231%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1232%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1233%')
                    AND ad_type IN ('interstitial_video', 'rewarded_video')
            ) C
            ON A.requestid = C.requestid
                AND A.campaign_id = C.campaign_id
            LEFT JOIN (
                SELECT requestid, campaign_id
                    , if(size(split(ext_bp, '\\"')) > 1, split(ext_bp, '\\"')[1], split(substr(ext_bp, 2), ',')[0]) AS rev_cpi
                    , expect_cost AS cost_cpi
                FROM dwh.ods_adn_trackingnew_install
                WHERE concat(yyyy, mm, dd, hh) >= '{0}'
                    AND concat(yyyy, mm, dd, hh) <= '{2}'
                   AND (get_json_object(ext_data2, '$.expIds') LIKE '%1231%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1232%'
                    OR get_json_object(ext_data2, '$.expIds') LIKE '%1233%')
                    AND ad_type IN ('interstitial_video', 'rewarded_video')
            ) D
            ON A.requestid = D.requestid
                AND A.campaign_id = D.campaign_id
    )
)
where cc='us' and client_type='nrs'
and unit_id in('1597036')
GROUP BY exp_id, devid_type, plf, cc,bidding_type,client_type,cam_type, unit_id, campaign_id, recall_rank
""".format(start_hour, end_hour, install_end_hour)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("saas-exp") \
        .enableHiveSupport() \
        .getOrCreate()
    
    df = spark.sql(sql)
    save_path = "s3a://mob-emr-test/wanjun/ab_exp/saas_exp" 
    df.select("*").write.option("header", "true").csv(save_path)

