#!/bin/bash

#/***************************************************************
# name: engine_real_abnormal_ratio.sh
# date: 2017-04-13
# author:
# desc:
#****************************************************************/

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
V_PATH_DATE=`date -d$V_DATE +%Y/%m/%d`
TABLE_NAME=dwm_transit_bm_data_rate
DATA_PATH=/user/galileo_data/data/bi/transit_dw/$TABLE_NAME/$V_PATH_DATE
TMP_DATA_PATH=/user/galileo_data/tmp/engine_real_abnormal_ratio/$V_PATH_DATE

ret=`hadoop fs -test -e $TMP_DATA_PATH`
if [ $? -ne 0 ]
then
    hadoop fs -mkdir -p $TMP_DATA_PATH
fi

while((1))
do
    ret=`hadoop fs -test -e $DATA_PATH/_SUCCESS`
    if [ $? -eq 0 ]
    then
        hadoop fs -rmr $DATA_PATH/_SUCCESS
        break
    else
        sleep 60
    fi
done

ret=`cat 3.txt | grep -v 'city_id' > 4.txt`

MAP_JAR_FILENAME="MapMerge.jar"
sql="
add jar $MAP_JAR_FILENAME;
create temporary function map_merge as 'com.hive.udf.MapMerge';
set mapreduce.job.queuename=root.bashishiyebu-yewuzhichengzhongxing.galileo_data;
use transit_dw;

create temporary table tmp_abnormal_ratio_$V_DATE(
    city_id string,
    real_abnormal_ratio double
)row format delimited fields terminated by '\t';

load data local inpath '4.txt' overwrite into table tmp_abnormal_ratio_$V_DATE;

create temporary table tmp_dwm_transit_bm_data_rate
as
select
    a.city_id,
    a.data_source_id,
    map_merge(statistical_data, map('real_abnormal_ratio', b.real_abnormal_ratio))
from dwm_transit_bm_data_rate a
left join tmp_abnormal_ratio_$V_DATE b
on a.city_id = b.city_id
where
    concat(year,month,day) = '$V_DATE';

alter table $TABLE_NAME drop partition(year='$V_YEAR',month='$V_MONTH',day='$V_DAY');
alter table $TABLE_NAME add partition(year='$V_YEAR',month='$V_MONTH',day='$V_DAY') location '$V_PATH_DATE';
insert overwrite table $TABLE_NAME partition(year='$V_YEAR',month='$V_MONTH',day='$V_DAY')
select
    *
from tmp_dwm_transit_bm_data_rate;
"

echo "$sql"
hive -e "$sql"

#success check
folder_size=`hadoop fs -du -s $DATA_PATH | awk '{print $1}'`
if [ $folder_size -gt 0 ]
then
    hadoop fs -touchz $DATA_PATH/_SUCCESS
    hadoop fs -touchz $TMP_DATA_PATH/_SUCCESS
    exit 0
else
    exit 255
fi
