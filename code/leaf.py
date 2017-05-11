#!/usr/local/bin/python
#encoding: utf-8

'''
推送统计报表数据到Leaf平台工具
注意：
hive sql 必须输出到指定文件中,并且sql的select中必须明确指明列名
并且列名建议使用英文，可以在leaf平台配置中英文转换
'''

import sys
import time
import json
import httplib
import urllib
import collections

FLOAT_PRECISION=2
#keep 2 precison
def precision_process(val):
    is_float = True

    idx = val.find('.')
    if idx == -1:
        is_float = False
        return is_float, val

    float_num = len(val) - idx - 1
    #data x.03
    if float_num <= FLOAT_PRECISION:
        return is_float, val

    #data x.03001
    effective_pos = idx + FLOAT_PRECISION + 1
    return is_float, val[0:effective_pos]

def format_val(val):
    is_float,val = precision_process(val)
    format_val = val
    if is_float:
        format_val = float(val)
    else:
        try:
            format_val = int(val)
        except:
            format_val = val
    return format_val

def post_data(params, timeout = 6000, retry_time = 3):
    host = '100.90.83.51'
    port = '8527'
    uri = '/api/transit/datainfo/add'
    connection = httplib.HTTPConnection(host, port, timeout)
    headers = {"Content-type":"application/json"}

    post_str = json.dumps(params)
    print post_str
    ret = 0
    for idx in range(0, retry_time):
        connection.request('POST', uri, post_str, headers)
        resp = connection.getresponse()
        if resp.status != 200:
            continue
        else:
            ret = 1
            break
    return ret


if __name__ == '__main__':
    if len(sys.argv) < 6:
        print 'usage: python push_leaf.py filename(数据所在文件) dim_field_num(维度个数) access_id(leaf接入id,leaf平台分配) access_key(leaf接入key,leaf平台分配) date(统计数据日期,ymd)'
        sys.exit(-1)

    filename = sys.argv[1]
    dim_field_num = int(sys.argv[2])
    access_id = int(sys.argv[3])
    access_key = sys.argv[4]
    timestamp = int(time.mktime(time.strptime(sys.argv[5],'%Y%m%d')))
    curr_data = {'access_id':access_id, 'access_key':access_key,'timestamp':timestamp, 'data':[]}

    titles = []
    f = file(filename)
    line_idx = 0
    while True:
        line_idx += 1
        line = f.readline()
        if len(line) == 0:
            break

        if line_idx == 1:
            titles = line.strip('\n').split('\t')
            continue

        cols = line.strip('\n').split('\t')
        if (cols == None) or (len(cols) <= dim_field_num):
            print 'error data: only dimension data'
            sys.exit(-1)

        params = collections.OrderedDict()
        for idx in range(0,dim_field_num):
            params[titles[idx]] = cols[idx]

        val_list = collections.OrderedDict()
        for idx in range(dim_field_num,len(cols)):
            val = cols[idx]
            if val == 'NULL':
                val_list[titles[idx]] = 0
                continue
            val_list[titles[idx]] = format_val(val)

        curr_record = {'params':params, 'val':val_list}
        curr_data['data'].append(curr_record)

        if line_idx % 50 == 0:
            post_data(curr_data)
            curr_data['data'] = []

    f.close()

    post_data(curr_data)
