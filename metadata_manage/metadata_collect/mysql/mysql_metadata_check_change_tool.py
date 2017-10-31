#!/usr/bin/env python
# -*- coding:utf-8 -*-
#监控mysql元数据变化，目前需要监控的内容如下：
#1、db级别：db的增加
#2、table级别：table的增加、table注释的变化
#3、column级别：column的增加、data_type的变化、column_type的变化、is_nullable的变化、column_default的变化、character_set_name的变化、collation_name的变化、column_comment的变化、ordinal_position的变化

import sys
import os
pwd = os.path.realpath(__file__)
print '/'.join(pwd.split('/')[:-3])
sys.path.append('/'.join(pwd.split('/')[:-3]))

from util.db_util import DBUtil
from util.date_util import DateUtil
from util.config_util import ConfigUtil
import  json

yesterday = DateUtil().get_yesterday()
today = DateUtil().get_current_date()

#db级别的监控:判断是否有新的db增加
def check_db_change(db_util):

    yesterday_dbs_sql = 'select host_name,port,db_name from business_db where version="' + yesterday +'"'
    yesterday_dbs = db_util.query(yesterday_dbs_sql)

    today_dbs_sql = 'select host_name,port,db_name from business_db where version="' + today + '"'
    today_dbs = db_util.query(today_dbs_sql)

    new_dbs = []

    if len(today_dbs) != len(yesterday_dbs):
        if len(today_dbs) >  len(yesterday_dbs): # 目前只判断数据库增加的情况，暂时不考虑数据库被删除的情况
            for today_db in today_dbs:
                today_db_key = today_db['host_name']+"::"+str(today_db['port'])+"::"+today_db['db_name']
                is_find_today_db_in_yesterday_dbs = False
                for yesterday_db in yesterday_dbs:
                    yesterday_db_key = yesterday_db['host_name']+"::"+str(yesterday_db['port'])+"::"+yesterday_db['db_name']
                    if today_db_key == yesterday_db_key:
                        is_find_today_db_in_yesterday_dbs = True
                        break
                # 定位到新增加的库，将其添加到 add_new_dbs 中
                if not is_find_today_db_in_yesterday_dbs:
                    new_dbs.append(today_db)

    if len(new_dbs) > 0:
        for new_db in new_dbs:
            msg = '新增加了db，具体信息为:host_name=' + new_db['host_name'] + ",port=" + str(new_db['port']) + ",db_name=" + new_db['db_name']
            print msg
            new_db_row = {}
            new_db_row['log_date'] = today
            new_db_row['operate_type'] = 'add'
            new_db_row['host_name'] = new_db['host_name']
            new_db_row['port'] = new_db['port']
            new_db_row['db_name'] = new_db['db_name']
            db_util.insert_dict_into_table('business_db_log',new_db_row)

#table级别的监控
#首先判断是否添加了新的table
#然后判断table的comment是否发生了变化
def check_table_change(db_util):

    today_dbs_sql = 'select host_name,port,db_name from business_db where version="' + today + '"'
    today_dbs = db_util.query(today_dbs_sql)
    #循环遍历检查所有数据库下的所有表的变化
    for db in today_dbs:
        db_name = db['db_name']
        host_name = db['host_name']
        port = db['port']
        tmp_business_table_sql = 'select * from business_table where db_name="'+db_name+'" and host_name="'+host_name+'" and port='+str(port)+' and version="'
        yesterday_table_sql = tmp_business_table_sql + yesterday +'"'
        today_table_sql = tmp_business_table_sql + today +'"'
        today_tables = db_util.query(today_table_sql)
        for today_table in today_tables:
            today_table_name = today_table['table_name']
            #检查该表是否是新增加的表
            check_table_exist_sql = tmp_business_table_sql + yesterday +'" and table_name="'+today_table_name+'"'
            rows = db_util.query(check_table_exist_sql)
            if len(rows) == 0:#某个表在昨天不存在，即该表是新增的
                new_table_log_row = {}
                new_table_log_row['log_date'] = today
                new_table_log_row['operate_type'] = 'add'
                new_table_log_row['db_name'] = db_name
                new_table_log_row['host_name'] = host_name
                new_table_log_row['port'] = port
                new_table_log_row['table_name'] = today_table_name
                new_table_log_row['change_before'] = '{}'
                new_table_log_row['change_after'] = json.dumps(today_table,ensure_ascii=False).decode('utf8')
                db_util.insert_dict_into_table('business_table_log',new_table_log_row)
                #该表的所有字段，都记录为新增
                add_new_table_all_columns(today_table,db_util)
            else:
                #判断表的comment是否发生了变化
                today_table_comment = today_table['table_comment']
                yesterday_table = rows[0]
                yesterday_table_comment = yesterday_table['table_comment']

                if today_table_comment <> yesterday_table_comment:
                    table_comment_change_log_row = {}
                    table_comment_change_log_row['log_date'] = today
                    table_comment_change_log_row['operate_type'] = 'change_comment'
                    table_comment_change_log_row['db_name'] = db_name
                    table_comment_change_log_row['host_name'] = host_name
                    table_comment_change_log_row['port'] = port
                    table_comment_change_log_row['table_name'] = today_table_name
                    table_comment_change_log_row['change_before'] = json.dumps(yesterday_table,ensure_ascii=False).decode('utf8')
                    table_comment_change_log_row['change_after'] = json.dumps(today_table,ensure_ascii=False).decode('utf8')
                    db_util.insert_dict_into_table('business_table_log', table_comment_change_log_row)

                check_column_change(today_table,db_util)

#column级别的监控
def check_column_change(table_row,db_util):
    # 检查存在的表的所有字段是否发生了变化，比如是否新增了字段
    db_name = table_row['db_name']
    host_name = table_row['host_name']
    port = table_row['port']
    table_name = table_row['table_name']

    tmp_business_column_sql = 'select * from business_column where db_name="' + db_name + '" and host_name="' + host_name + '" and port=' + str(port) + ' and table_name="' + table_name + '" and version="'


    # 检查是否有某些列被删除了，即在昨天有记录，但是今天没有了记录
    check_if_column_deleted(tmp_business_column_sql,db_util)

    #取出某个table下的所有列
    today_table_column_sql = tmp_business_column_sql + today + '"'
    today_table_columns = db_util.query(today_table_column_sql)

    for today_table_column in today_table_columns:
        today_column_name = today_table_column['column_name']
        #检查 某个字段 是否是新增的
        check_column_exist_sql = tmp_business_column_sql + yesterday + '" and column_name="' + today_column_name + '"'
        rows = db_util.query(check_column_exist_sql)
        if len(rows) == 0:#如果某个字段在昨天不存在，则该字段是新增的字段
            new_column_log_row = {}
            new_column_log_row['log_date'] = today
            new_column_log_row['operate_type'] = 'add'
            new_column_log_row['db_name'] = db_name
            new_column_log_row['host_name'] = host_name
            new_column_log_row['port'] = port
            new_column_log_row['table_name'] = table_name
            new_column_log_row['column_name'] = today_column_name
            new_column_log_row['change_before'] = '{}'
            new_column_log_row['change_after'] = json.dumps(today_table_column,ensure_ascii=False).decode('utf8')
            db_util.insert_dict_into_table('business_column_log', new_column_log_row)
        else:#如果某个字段在昨天存在，则检查 字段 的哪些信息发生了变化
            yesterday_table_column = rows[0]

            check_column_property_chage(yesterday_table_column, today_table_column, 'data_type', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'column_type', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'is_nullable', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'column_default', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'character_set_name', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'collation_name', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'column_comment', db_util)
            check_column_property_chage(yesterday_table_column, today_table_column, 'ordinal_position', db_util)


def check_if_column_deleted(tmp_business_column_sql,db_util):
    yesterday_table_column_sql = tmp_business_column_sql + yesterday + '"'
    yesterday_table_columns = db_util.query(yesterday_table_column_sql)

    for yesterday_table_column in yesterday_table_columns:
        yesterday_column_name = yesterday_table_column['column_name']
        # 检查 某个字段 是否是删除掉了
        check_column_exist_sql = tmp_business_column_sql + today + '" and column_name="' + yesterday_column_name + '"'
        rows = db_util.query(check_column_exist_sql)
        if len(rows) ==0:#如果某个字段在昨天存在,但是在今天不存在,则该字段是被删除的字段
            deleted_column_log_row = {}
            deleted_column_log_row['log_date'] = today
            deleted_column_log_row['operate_type'] = 'delete'
            deleted_column_log_row['db_name'] = yesterday_table_column['db_name']
            deleted_column_log_row['host_name'] = yesterday_table_column['host_name']
            deleted_column_log_row['port'] = yesterday_table_column['port']
            deleted_column_log_row['table_name'] = yesterday_table_column['table_name']
            deleted_column_log_row['column_name'] = yesterday_table_column['column_name']
            deleted_column_log_row['change_before'] = json.dumps(yesterday_table_column, ensure_ascii=False).decode('utf8')
            deleted_column_log_row['change_after'] = '{}'
            db_util.insert_dict_into_table('business_column_log', deleted_column_log_row)





def check_column_property_chage(yesterday_table_column,today_table_column,property,db_util):
    today_column_property = today_table_column[property]
    yesterday_column_property = yesterday_table_column[property]
    operate_type = 'change_'+property
    if today_column_property <> yesterday_column_property:
        column_change_log_row = {}
        column_change_log_row['log_date'] = today
        column_change_log_row['operate_type'] = operate_type
        column_change_log_row['db_name'] = today_table_column['db_name']
        column_change_log_row['host_name'] = today_table_column['host_name']
        column_change_log_row['port'] = today_table_column['port']
        column_change_log_row['table_name'] = today_table_column['table_name']
        column_change_log_row['column_name'] = today_table_column['column_name']
        column_change_log_row['change_before'] = json.dumps(yesterday_table_column,ensure_ascii=False).decode('utf8')
        column_change_log_row['change_after'] = json.dumps(today_table_column,ensure_ascii=False).decode('utf8')
        db_util.insert_dict_into_table('business_column_log', column_change_log_row)



#当某个表新增时，将该表的所有字段信息添加到business_column_log
def add_new_table_all_columns(table_row,db_util):
    db_name = table_row['db_name']
    host_name = table_row['host_name']
    port = table_row['port']
    table_name = table_row['table_name']
    columns_sql = 'select * from business_column where db_name ="'+db_name+'" and host_name="'+host_name+'" and port='+str(port)+' and table_name="'+table_name+'"'
    columns = db_util.query(columns_sql)
    for column in columns:
        new_column_log_row = {}
        new_column_log_row['log_date'] = today
        new_column_log_row['operate_type'] = 'add_table'
        new_column_log_row['db_name'] = db_name
        new_column_log_row['host_name'] = host_name
        new_column_log_row['port'] = port
        new_column_log_row['table_name'] = table_name
        new_column_log_row['column_name'] = column['column_name']
        new_column_log_row['change_before'] ='{}'
        new_column_log_row['change_after'] = json.dumps(column,ensure_ascii=False).decode('utf8')
        db_util.insert_dict_into_table('business_column_log',new_column_log_row)


def delete_metadata(db_util):
    delete_metadata_by_tablename('business_db',yesterday,db_util)
    delete_metadata_by_tablename('business_table', yesterday,db_util)
    delete_metadata_by_tablename('business_column', yesterday,db_util)

def delete_metadata_by_tablename(table_name,version,db_util):
    delete_sql = 'delete from ' +table_name +' where version="' + version +'"'
    db_util.delete(delete_sql)

def delete_metadata_checklog(db_util):
    delete_metadata_checklog_by_tablename('business_db_log',today,db_util)
    delete_metadata_checklog_by_tablename('business_table_log', today, db_util)
    delete_metadata_checklog_by_tablename('business_column_log', today, db_util)

def delete_metadata_checklog_by_tablename(table_name,log_date,db_util):
    delete_sql = 'delete from ' + table_name + ' where log_date="' + log_date + '"'
    db_util.delete(delete_sql)



if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf8')
    env = 'local'
    if len(sys.argv) == 2:
        env = sys.argv[1]

    configUtil = ConfigUtil(env)

    business_db_meta_dbutil = DBUtil(configUtil.get_property_value('business_db_meta.host'),
                                     configUtil.get_property_value('business_db_meta.port'),
                                     configUtil.get_property_value('business_db_meta.username'),
                                     configUtil.get_property_value('business_db_meta.password'),
                                     configUtil.get_property_value('business_db_meta.db')
                                     )
    # 删除log_date等于今天的记录
    delete_metadata_checklog(business_db_meta_dbutil)
    # 检查db的变化，并将变化的内容入库
    check_db_change(business_db_meta_dbutil)
    # 检查table的变化，并将变化的内容入库
    check_table_change(business_db_meta_dbutil)
    # 最终需要删除昨天的版本
    delete_metadata(business_db_meta_dbutil)
