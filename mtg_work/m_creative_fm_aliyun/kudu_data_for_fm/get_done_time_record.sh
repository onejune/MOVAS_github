#!/bin/sh

get_done_time_record(){
    last_day=`hadoop fs -ls ${1} | awk -F "/" '{print $NF}' | grep '20' | tail -n 1`
    last_hour=`hadoop fs -ls ${1}/${last_day} | tail -n 1 | awk -F "/" '{print $NF}'`
    cnt=0
    while true
    do
        hadoop dfs -test -f ${1}/${last_hour:0:8}/${last_hour:0:10}/_SUCCESS
        if [[ $? -eq 0 ]]; then
            break
        fi

        let cnt+=1
        if [[ ${cnt} -ge 24 ]]; then
            echo "until ${last_hour}, no data found!"
            exit 1
        fi

        last_hour=`TZ=Asia/Shanghai date -d "-1 hours ${last_hour:0:8} ${last_hour:8:2}" "+%Y%m%d%H"`
    done
    # echo `date -d "${last_hour:0:4}-${last_hour:4:2}-${last_hour:6:2} ${last_hour:8:2}:10:00" +%s`
    echo $last_hour
}

# done_time=$(get_done_time_record "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process_tag")
# echo $done_time
