#!/bin/bash
source /etc/profile
if [ x$1 = x ]
then
    S_DATE=`date -d"1 day ago" +%Y%m%d`
else
    S_DATE=$1
fi
V_YEAR=`date -d "$V_DATE" +%Y`
V_MONTH=`date -d "$V_DATE" +%m`
V_DAY=`date -d "$V_DATE" +%d`
V_DATE_F=`date -d "$V_DATE" +%Y-%m-%d`

DATA_PATH=/user/galileo_data/tmp/dwv_bi_transit_line_records/$V_YEAR/$V_MONTH/$V_DAY

SQL_STR="add jar /home/busbi/common/lib/jsonvisitor-1.0.jar;
create temporary function js_len as 'com.didi.hive.udf.JsonLen';
set mapreduce.job.queuename=root.bashishiyebu-yewuzhichengzhongxing.busbi;
insert overwrite directory '$DATA_PATH'
select a.city as city,
       b.action as action,
       b.line_id as line_id,
       b.line_quality_flag as line_quality_flag,
       b.req_time as req_time,
       b.imei as imei
from
    (select city_id as city
    from transit_dw.dim_transit_city
    where is_dhundred_flag=1
    ) a
join
    (select a.action as action,
            a.line_quality_flag as line_quality_flag,
            a.line_id as line_id,
            a.req_time as req_time,
            a.imei as imei,
            a.city as city
    from 
        (select case when param['action']='line_query' then 1 when param['action']='line_bus_location' then 0 else -1 end as action,
                hour(param['timestamp'])*3600+minute(param['timestamp'])*60+second(param['timestamp']) as req_time,
                param['imei'] as imei,
                param['city'] as city,
                case when param['action'] = 'line_bus_location' then get_json_object(param['lines'],'$.[0].line.line_id') 
                     when param['action'] = 'line_query' then param['pattern'] 
                     else 0 end as line_id,
                case when param['action'] = 'line_query' then get_json_object(param['lines'],'$.[0].realtime_available')
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') = 2 and js_len(get_json_object(param['lines'],'$.[0].buses')) = 0 then 2
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') = 2 and js_len(get_json_object(param['lines'],'$.[0].buses')) > 0 then 3
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') = 3 then 4
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') = 4 and get_json_object(param['lines'],'$.[0].line.state') = 0 then 5
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') = 4 and get_json_object(param['lines'],'$.[0].line.state') = -1 then 6
                     when param['action'] = 'line_bus_location' and (get_json_object(param['lines'],'$.[0].line.signal_icon') = 4 and get_json_object(param['lines'],'$.[0].line.state') = -3) or get_json_object(param['lines'],'$.[0].line.signal_icon') = 1 then 7
                     when param['action'] = 'line_bus_location' and get_json_object(param['lines'],'$.[0].line.signal_icon') is null then 8
                     else 9 end as line_quality_flag
        from transit_dw.ods_log_transit_line_records 
        where concat(year,'-',month,'-',day) = '${V_DATE_F}'
            and param['line_count'] = 1
            and (param['action'] = 'line_query' or (param['action']='line_bus_location' and get_json_object(param['lines'],'$.[0].line.line_id') is not null))
            and param['imei'] is not null
            and param['imei'] <> ''
            and param['imei'] not in ('crawl_comp', 'OD_query')
        ) a
    join
        (select param['imei'] as imei,
                param['city'] as city
        from transit_dw.ods_log_transit_line_records
        where concat(year,'-',month,'-',day) = '${V_DATE_F}'
            and param['action'] in ('line_query') 
            and param['imei'] is not NULL
            and param['imei'] <> ''
            and param['imei'] not in ('crawl_comp', 'OD_query')
        ) b
    on a.imei=b.imei and a.city=b.city
    group by a.action,a.line_quality_flag,a.line_id,a.req_time,a.imei,a.city
    ) b
on a.city=b.city
"
hive -e "${SQL_STR}">1.txt
