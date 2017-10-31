#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 分别检查 business_db(table、column)_log,然后对发生的变化发送报警短信

import sys
import os
pwd = os.path.realpath(__file__)
print '/'.join(pwd.split('/')[:-3])
sys.path.append('/'.join(pwd.split('/')[:-3]))

from util.db_util import DBUtil
from util.date_util import DateUtil
from util.config_util import ConfigUtil
from util.sms_util import SmsUtil
import  json

today = DateUtil().get_current_date()

def monitor_mysql_metadata_change_log(db_util):
    monitor_db_change_log(db_util)
    monitor_table_change_log(db_util)
    monitor_column_change_log(db_util)


def monitor_db_change_log(db_util):
    query_sql = 'select host_name,port,db_name,operate_type from business_db_log where log_date="' + today +'"'
    rows = db_util.query(query_sql)
    if len(rows) > 0:
        for row in rows:
            msg = 'db_metadata_change:::host_name='+row['host_name']+',port='+str(row['port'])+',db_name='+row['db_name']+',operate_type='+row['operate_type']
            print msg
            # sms_util.send_msg(msg)

def monitor_table_change_log(db_util):
    query_sql = 'select host_name,port,db_name,table_name,operate_type from business_table_log where log_date="' + today + '"'
    rows = db_util.query(query_sql)
    if len(rows) > 0:
        for row in rows:
            msg = 'table_metadata_change:::host_name=' + row['host_name'] + ',port=' + str(row['port']) + ',db_name=' + row['db_name'] +',table_name='+row['table_name']+ ',operate_type=' + row['operate_type']
            print msg
            # sms_util.send_msg(msg)

def monitor_column_change_log(db_util):
    query_sql = 'select host_name,port,db_name,table_name,column_name,operate_type from business_column_log where log_date="' + today + '"'
    rows = db_util.query(query_sql)
    if len(rows) > 0:
        for row in rows:
            msg = 'column_metadata_change:::host_name=' + row['host_name'] + ',port=' + str(row['port']) + ',db_name=' + row['db_name'] + ',table_name=' + row['table_name'] +',column_name='+row['column_name']+ ',operate_type=' + row['operate_type']
            print msg
            # sms_util.send_msg(msg)



if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    env = 'local'
    if len(sys.argv) == 2:
        env = sys.argv[1]

    configUtil = ConfigUtil(env)
    sms_util = SmsUtil(env)

    business_db_meta_dbutil = DBUtil(configUtil.get_property_value('business_db_meta.host'),
                                     configUtil.get_property_value('business_db_meta.port'),
                                     configUtil.get_property_value('business_db_meta.username'),
                                     configUtil.get_property_value('business_db_meta.password'),
                                     configUtil.get_property_value('business_db_meta.db')
                                     )
    monitor_mysql_metadata_change_log(business_db_meta_dbutil)


