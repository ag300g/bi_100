#!/bin/sh

if [ x$1 = x ]
then
    V_DATE=`date -d" 1 day ago" +%Y%m%d`
else
    V_DATE=$1
fi
source /etc/profile
export HADOOP_USER_NAME='galileo_data'

V_YEAR=`date -d$V_DATE +%Y`
V_MONTH=`date -d$V_DATE +%m`
V_DAY=`date -d$V_DATE +%d`

sql="
set mapreduce.job.queuename=root.bashishiyebu-yewuzhichengzhongxing.galileo_data;
set hive.cli.print.header=false;

use transit_dw;
select 
    7 as city_id,
    round(100 * abnormal_num / total_num, 2) as real_abnormal_ratio 
from 
(
    select 
        sum(case when (bloom_resp['status'] = 1) or (cast(bloom_resp['normal_num'] as int) > 0 and cast(bloom_resp['nogps_num'] as int) / cast(bloom_resp['normal_num'] as int) > 0.6) then 1 else 0 end) as abnormal_num, 
        count(1) as total_num
    from dwv_log_transit_realtime_query 
    where 
        concat(year,month,day) = '$V_DATE' 
        and city_id = '7'
)q
"

echo "$sql"
hive -e "$sql" >> 3.txt
